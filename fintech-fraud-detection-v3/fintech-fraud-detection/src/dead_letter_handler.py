"""
=============================================================================
Dead-Letter Handler — ClearPay UK Fraud Detection Platform
=============================================================================
Captures malformed, unparseable, or schema-violating events from the
main streaming pipeline and routes them to a dedicated dead-letter
container in ADLS Gen2 for manual review and reprocessing.

Why this matters:
  - In a payment network, a dropped event could be a real fraud transaction
    that was simply malformed. We NEVER discard silently — every failed
    event is captured with full diagnostic metadata.
  - MLR 2017 requires an audit trail. If a high-value transaction failed
    to process, the compliance team must be notified within 15 minutes.
=============================================================================
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import logging

log = logging.getLogger("ClearPay.DeadLetter")

DEAD_LETTER_PATH = (
    "abfss://gold@{storage_account}.dfs.core.windows.net/dead_letter/"
)


def capture_dead_letter_events(raw_stream: DataFrame) -> DataFrame:
    """
    Identify events that failed schema parsing (null Transaction_ID after
    from_json) and tag them with failure reason and diagnostic metadata.

    These events are written to gold/dead_letter/ for ops team review.
    An Azure Monitor alert fires if dead-letter volume exceeds 10/minute.
    """
    dead_letter = (
        raw_stream
        # Events where mandatory fields are null = JSON parse failure
        .filter(
            F.col("Transaction_ID").isNull() |
            F.col("Event_Timestamp").isNull() |
            F.col("Transaction_Amount_GBP").isNull()
        )
        .withColumn("failure_reason",
            F.when(F.col("Transaction_ID").isNull(), "NULL_TRANSACTION_ID")
             .when(F.col("Event_Timestamp").isNull(), "NULL_EVENT_TIMESTAMP")
             .when(F.col("Transaction_Amount_GBP").isNull(), "NULL_AMOUNT")
             .otherwise("SCHEMA_MISMATCH")
        )
        .withColumn("dead_letter_timestamp", F.current_timestamp())
        .withColumn("dead_letter_date",      F.to_date(F.current_timestamp()))
        .withColumn("pipeline_name",         F.lit("fraud_detection_stream"))
        .withColumn("review_status",         F.lit("PENDING"))
        .withColumn("compliance_priority",
            # Flag potential high-value dead letters for urgent review
            F.when(
                F.col("body_str").contains("Transaction_Amount_GBP") &
                F.col("body_str").rlike(r'"Transaction_Amount_GBP":\s*[5-9][0-9]{3,}'),
                "HIGH"
            ).otherwise("NORMAL")
        )
    )

    log.info("Dead-letter stream configured. Events routed to gold/dead_letter/")
    return dead_letter


def write_dead_letter_stream(dead_letter_stream: DataFrame, storage_account: str,
                              checkpoint_path: str, trigger_interval: str = "30 seconds"):
    """
    Write dead-letter events to gold/dead_letter/ Delta table.
    Partitioned by dead_letter_date for efficient querying.
    """
    path = DEAD_LETTER_PATH.format(storage_account=storage_account)

    query = (
        dead_letter_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("dead_letter_date")
        .trigger(processingTime=trigger_interval)
        .start(path)
    )

    log.info("Dead-letter write stream started → %s", path)
    return query
