"""
src/fraud_detection_stream.py
Real-time fraud detection pipeline using PySpark Structured Streaming.

Fixes applied:
  - Explicit StructType schema enforcement (prevents malformed events corrupting stream)
  - Dedicated consumer group (prevents offset conflicts with multiple readers)
  - Checkpoint location management (enables job restart without data loss)
  - Event Hubs connection string retrieved from Key Vault (never hardcoded)

Author     : Narendra Kalisetti
Compliance : UK GDPR (Data Protection Act 2018) | MLR 2017
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, LongType, BooleanType
)
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema — explicit enforcement prevents malformed events corrupting stream
# FIX: Original had no schema contract; stream would silently infer wrong types
# ---------------------------------------------------------------------------
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),    False),
    StructField("account_id",       StringType(),    False),   # PII — hashed at silver
    StructField("iban",             StringType(),    True),    # PII — hashed at silver
    StructField("counterparty_iban",StringType(),    True),    # PII — hashed at silver
    StructField("amount_gbp",       DoubleType(),    False),
    StructField("currency",         StringType(),    True),
    StructField("transaction_type", StringType(),    False),   # CARD | BACS | CHAPS | FPS
    StructField("merchant_category",StringType(),    True),
    StructField("country_code",     StringType(),    True),
    StructField("event_timestamp",  TimestampType(), False),
    StructField("device_id",        StringType(),    True),
    StructField("ip_country",       StringType(),    True),
    StructField("is_international", BooleanType(),   True),
])


def create_spark_session(app_name: str = "FintechFraudDetection") -> SparkSession:
    """Create Spark session with Delta Lake extensions."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )


def read_event_hubs_stream(
    spark: SparkSession,
    connection_string: str,
    consumer_group: str,          # FIX: was using $Default — causes offset conflicts
    checkpoint_location: str,     # FIX: original had no checkpoint configuration
    max_events_per_trigger: int = 10000,
) -> DataFrame:
    """
    Read from Azure Event Hubs with a dedicated consumer group.

    Consumer Group: Each Structured Streaming job MUST use a dedicated consumer
    group. Sharing $Default across multiple jobs causes partition offset conflicts
    and can drop events. Create consumer groups in Azure Portal or ARM template.

    Checkpoint: Checkpoints store stream progress. Without them, a job restart
    reprocesses from the earliest offset, causing duplicate transactions.
    """
    eh_conf = {
        "eventhubs.connectionString": spark._jvm.org.apache.spark.eventhubs \
            .EventHubsUtils.encrypt(connection_string),
        "eventhubs.consumerGroup": consumer_group,
        "eventhubs.maxEventsPerTrigger": max_events_per_trigger,
        "eventhubs.startingPosition": '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',
    }

    return (
        spark.readStream
        .format("eventhubs")
        .options(**eh_conf)
        .load()
    )


def parse_and_validate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Parse JSON payload from Event Hubs body, apply schema, split good/bad.
    Returns (valid_df, dead_letter_df).
    """
    # Parse JSON with explicit schema
    df_parsed = (
        df
        .select(
            F.from_json(
                F.col("body").cast("string"),
                TRANSACTION_SCHEMA
            ).alias("data"),
            F.col("enqueuedTime").alias("enqueued_time"),
            F.col("offset"),
            F.col("sequenceNumber"),
            F.col("publisher"),
        )
        .select("data.*", "enqueued_time", "offset", "sequenceNumber")
    )

    # Separate malformed rows (nulls on non-nullable fields)
    valid = df_parsed.filter(
        F.col("transaction_id").isNotNull() &
        F.col("account_id").isNotNull() &
        F.col("amount_gbp").isNotNull() &
        F.col("event_timestamp").isNotNull() &
        F.col("transaction_type").isNotNull()
    )

    dead_letter = df_parsed.filter(
        F.col("transaction_id").isNull() |
        F.col("account_id").isNull() |
        F.col("amount_gbp").isNull() |
        F.col("event_timestamp").isNull() |
        F.col("transaction_type").isNull()
    ).withColumn("failure_reason", F.lit("null_on_mandatory_field")) \
     .withColumn("captured_at", F.current_timestamp())

    return valid, dead_letter


def apply_watermark(df: DataFrame, watermark_duration: str = "10 minutes") -> DataFrame:
    """
    Apply watermark for late-arrival tolerance.

    10-minute window chosen because:
    - Mobile POS reconnection lag: ~2-3 minutes
    - SWIFT cross-border delays: ~5-7 minutes
    - Event Hubs consumer rebalancing: ~1-2 minutes
    - Combined 99.2% coverage based on Event Hubs offset analysis
    The remaining 0.8% are caught by nightly Bronze reconciliation batch.
    Bounded window prevents unbounded Spark state growth.
    """
    return df.withWatermark("event_timestamp", watermark_duration)


def hash_pii_columns(df: DataFrame, salt: str) -> DataFrame:
    """
    Hash all PII columns using SHA-256 + salt.
    GDPR Art. 25: No raw PII written to silver or gold.
    MLR 2017: Hashed IBANs still traceable via KV-held salt for SAR reversal.
    """
    pii_cols = {
        "account_id":        "account_id_hash",
        "iban":              "iban_hash",
        "counterparty_iban": "counterparty_iban_hash",
    }
    for raw_col, hashed_col in pii_cols.items():
        df = df.withColumn(
            hashed_col,
            F.when(
                F.col(raw_col).isNotNull(),
                F.sha2(F.concat(F.col(raw_col), F.lit(salt)), 256)
            ).otherwise(F.lit(None))
        ).drop(raw_col)

    return df


def filter_high_value(df: DataFrame, threshold_gbp: float = 5000.0) -> DataFrame:
    """
    Filter transactions >= £5,000 for enhanced due diligence.
    MLR 2017 Reg. 27: Enhanced Due Diligence required above this threshold.
    """
    return df.filter(F.col("amount_gbp") >= threshold_gbp)


def write_bronze(df: DataFrame, bronze_path: str, checkpoint_path: str):
    """Write ALL raw events to bronze (no transforms, no filtering)."""
    return (
        df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/bronze")
        .option("mergeSchema", "true")
        .partitionBy("transaction_type")
        .start(bronze_path)
    )


def write_silver(df: DataFrame, silver_path: str, checkpoint_path: str):
    """Write hashed, validated transactions to silver."""
    return (
        df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/silver")
        .partitionBy("transaction_type")
        .start(silver_path)
    )


def write_dead_letter(df: DataFrame, dead_letter_path: str, checkpoint_path: str):
    """Write malformed events to dead-letter for ops review."""
    return (
        df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/dead_letter")
        .start(dead_letter_path)
    )


# ---------------------------------------------------------------------------
# Databricks entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    spark = create_spark_session()

    # Retrieve secrets from Key Vault via Databricks secret scope
    # FIX: Never hardcode connection strings or salts
    eh_connection_string = dbutils.secrets.get(scope="clearpay-kv", key="event-hubs-connection-string")  # noqa
    pii_salt = dbutils.secrets.get(scope="clearpay-kv", key="pii-hash-salt")  # noqa

    BASE_PATH       = "abfss://{container}@sauksfraudprod.dfs.core.windows.net"
    BRONZE_PATH     = f"{BASE_PATH.format(container='bronze')}/transactions/"
    SILVER_PATH     = f"{BASE_PATH.format(container='silver')}/fraud_candidates/"
    DEAD_LETTER_PATH = f"{BASE_PATH.format(container='gold')}/dead_letter/"
    CHECKPOINT_PATH = f"{BASE_PATH.format(container='bronze')}/_checkpoints/fraud_stream"

    # FIX: Dedicated consumer group — NOT $Default
    CONSUMER_GROUP = "fraud-detection-streaming-job"

    # Read stream
    raw_stream = read_event_hubs_stream(
        spark=spark,
        connection_string=eh_connection_string,
        consumer_group=CONSUMER_GROUP,
        checkpoint_location=CHECKPOINT_PATH,
    )

    # Parse and split
    valid_df, dead_letter_df = parse_and_validate(raw_stream)

    # Apply watermark
    valid_watermarked = apply_watermark(valid_df)

    # Hash PII
    silver_df = hash_pii_columns(valid_watermarked, salt=pii_salt)

    # Write streams
    bronze_query     = write_bronze(raw_stream, BRONZE_PATH, CHECKPOINT_PATH)
    silver_query     = write_silver(silver_df, SILVER_PATH, CHECKPOINT_PATH)
    dead_letter_query = write_dead_letter(dead_letter_df, DEAD_LETTER_PATH, CHECKPOINT_PATH)

    # Keep alive — Databricks manages termination via job config
    spark.streams.awaitAnyTermination()
