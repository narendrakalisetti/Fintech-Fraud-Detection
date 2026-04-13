"""
src/dead_letter_handler.py
Handles malformed events from the streaming pipeline.
Routes to gold/dead_letter/ for ops review and alerting.

Author : Narendra Kalisetti
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def classify_failure(df: DataFrame) -> DataFrame:
    """
    Classify dead-letter events by failure reason for easier ops triage.
    """
    return df.withColumn(
        "failure_category",
        F.when(F.col("transaction_id").isNull(), "MISSING_TRANSACTION_ID")
         .when(F.col("amount_gbp").isNull(), "MISSING_AMOUNT")
         .when(F.col("event_timestamp").isNull(), "MISSING_TIMESTAMP")
         .when(F.col("account_id").isNull(), "MISSING_ACCOUNT_ID")
         .when(F.col("transaction_type").isNull(), "MISSING_TXN_TYPE")
         .otherwise("SCHEMA_MISMATCH")
    ).withColumn(
        "captured_at", F.current_timestamp()
    ).withColumn(
        "review_status", F.lit("PENDING")
    )


def write_dead_letter_stream(
    df: DataFrame,
    dead_letter_path: str,
    checkpoint_path: str,
) -> object:
    """Write dead-letter stream to Delta gold/dead_letter/ path."""
    classified = classify_failure(df)
    return (
        classified
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/dead_letter")
        .partitionBy("failure_category")
        .start(dead_letter_path)
    )
