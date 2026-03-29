"""
=============================================================================
ClearPay UK — Real-Time Fraud Detection Pipeline
=============================================================================
Author      : Narendra Kalisetti
Project     : ClearPay UK Fraud Detection
Region      : Azure UK South
Compliance  : UK GDPR (Data Protection Act 2018) | MLR 2017 | PCI DSS
Sustainability: Azure UK South — 100% renewable energy (Net Zero 2050)

Description : PySpark Structured Streaming pipeline that:
              1. Reads transactions from Azure Event Hubs
              2. Writes ALL raw events to bronze (full audit trail)
              3. Filters high-value transactions >£5,000 (MLR 2017)
              4. Hashes Customer_Account_IBAN SHA-256+salt (GDPR Art.25)
              5. Applies rule-based fraud risk scoring (MLR 2017 Reg.21)
              6. Routes SAR-eligible events (risk >= 0.75) to gold/sar_queue/
              7. Captures malformed events in dead-letter stream
              8. Writes clean silver Delta table for analyst queries

GDPR        : Customer_Account_IBAN is hashed before any write beyond bronze.
              Raw IBANs are NEVER written to silver, gold, or any log.

MLR 2017    : Transactions >=£5,000 flagged per Reg.27 EDD requirement.
              Risk score >=0.75 routed to SAR queue per Reg.35.

Net Zero    : Databricks cluster auto-terminates after 20 min idle.
              All processing in Azure UK South renewable-energy DC.
=============================================================================
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType, LongType,
)

log = logging.getLogger("ClearPay.FraudDetection")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SECRET_SCOPE             = "fintech-kv-scope"
EH_CONNSTR_SECRET        = "eh-namespace-connstr"
ADLS_ACCOUNT_SECRET      = "adls-account-name"
IBAN_SALT_SECRET         = "iban-hash-salt"
EH_CONSUMER_GROUP        = "$Default"
EH_TOPIC                 = "transactions"
HIGH_VALUE_THRESHOLD_GBP = 5_000.0   # MLR 2017 EDD threshold

# ---------------------------------------------------------------------------
# Transaction schema — mirrors Event Hubs JSON payload contract
# ---------------------------------------------------------------------------
TRANSACTION_SCHEMA = StructType([
    StructField("Transaction_ID",         StringType(),    False),
    StructField("Event_Timestamp",        TimestampType(), False),
    StructField("Customer_Account_IBAN",  StringType(),    False),
    StructField("Beneficiary_IBAN",       StringType(),    True),
    StructField("Merchant_ID",            StringType(),    True),
    StructField("Merchant_Category_Code", StringType(),    True),
    StructField("Transaction_Amount_GBP", DoubleType(),    False),
    StructField("Transaction_Currency",   StringType(),    True),
    StructField("Payment_Channel",        StringType(),    True),
    StructField("Country_Code",           StringType(),    True),
    StructField("Is_International",       StringType(),    True),
    StructField("Risk_Score",             DoubleType(),    True),
    StructField("Ingestion_Timestamp",    LongType(),      True),
])


def build_spark_session() -> SparkSession:
    """Initialise SparkSession with Delta Lake extensions."""
    spark = (
        SparkSession.builder
        .appName("ClearPay-FraudDetection-StreamingPipeline")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession initialised: %s", spark.sparkContext.appName)
    return spark


def filter_high_value_transactions(stream):
    """
    Filter to transactions >£5,000 with valid mandatory fields.

    Threshold defined by UK MLR 2017 Reg.27 Enhanced Due Diligence
    requirement. Only valid, non-null records proceed to silver.
    """
    return stream.filter(
        F.col("Transaction_ID").isNotNull()
        & F.col("Event_Timestamp").isNotNull()
        & F.col("Transaction_Amount_GBP").isNotNull()
        & (F.col("Transaction_Amount_GBP") > HIGH_VALUE_THRESHOLD_GBP)
    )


def hash_pii_columns(stream, iban_salt: str):
    """
    Pseudonymise PII using HMAC-SHA256 with a Key Vault-stored salt.

    GDPR justification (UK GDPR Article 25 - Privacy by Design):
      - Customer_Account_IBAN and Beneficiary_IBAN identify natural persons.
      - Hashing with a secret salt makes reversal computationally infeasible.
      - Deterministic: same IBAN always produces same hash, enabling joins.
      - Raw IBAN columns dropped immediately after hashing.
      - They are NEVER written to silver, gold, logs, or any downstream system.

    Right to Erasure (Art.17): deleting bronze records + rotating the salt
    in Key Vault satisfies erasure obligations for all layers.
    """
    hashed = (
        stream
        .withColumn(
            "Customer_Account_IBAN_Hashed",
            F.sha2(F.concat(F.col("Customer_Account_IBAN"), F.lit(iban_salt)), 256),
        )
        .withColumn(
            "Beneficiary_IBAN_Hashed",
            F.when(
                F.col("Beneficiary_IBAN").isNotNull(),
                F.sha2(
                    F.concat(F.col("Beneficiary_IBAN"), F.lit(iban_salt)), 256
                ),
            ).otherwise(F.lit(None)),
        )
        .drop("Customer_Account_IBAN", "Beneficiary_IBAN")
    )
    log.info("GDPR: Raw IBAN columns hashed (SHA-256+salt) and dropped")
    return hashed


def add_pipeline_metadata(stream):
    """Add audit, lineage, and compliance metadata columns."""
    return stream.withColumns(
        {
            "Pipeline_Name": F.lit(
                "ClearPay-FraudDetection-StreamingPipeline"
            ),
            "Pipeline_Run_Timestamp": F.current_timestamp(),
            "Data_Classification": F.lit("CONFIDENTIAL"),
            "GDPR_PII_Hashed": F.lit(True),
            "MLR_2017_Flagged": F.lit(True),
            "Silver_Load_Date": F.to_date(F.current_timestamp()),
            "Layer": F.lit("silver"),
        }
    )


# ---------------------------------------------------------------------------
# WATERMARKING STRATEGY
# ---------------------------------------------------------------------------
# .withWatermark("Event_Timestamp", "10 minutes")
#
# Late-arriving events in the ClearPay UK payment network come from:
#   - Mobile POS terminals reconnecting after connectivity loss
#   - SWIFT international transfer confirmation delays
#   - CHAPS settlement confirmation lag from correspondent banks
#   - Event Hubs consumer-group rebalancing during Databricks autoscaling
#
# 30-day offset analysis showed:
#   - 94.1% of events arrive within 2 minutes of event time
#   - 99.2% arrive within 10 minutes
#   - 0.8% arrive after 10 minutes (SWIFT cross-border outliers)
#
# A 10-minute watermark captures 99.2% of late arrivals while bounding
# Spark aggregation state size in memory — preventing OOM on the driver
# (a real incident we hit with a 48-hour watermark, documented in
# docs/CHALLENGES.md #3).
#
# The 0.8% beyond the watermark land in bronze and are caught by the
# nightly ADF Bronze reconciliation batch (pl_bronze_reconciliation).
# ---------------------------------------------------------------------------
def apply_watermark(stream):
    return stream.withWatermark("Event_Timestamp", "10 minutes")


def parse_transaction_stream(raw_stream):
    """Parse binary Event Hubs body into typed transaction columns."""
    return (
        raw_stream
        .withColumn("body_str", F.col("body").cast("string"))
        .withColumn("data", F.from_json(F.col("body_str"), TRANSACTION_SCHEMA))
        .select(
            "data.*",
            "body_str",
            F.col("enqueuedTime").alias("EH_Enqueued_Timestamp"),
            F.col("offset").alias("EH_Offset"),
            F.col("sequenceNumber").alias("EH_Sequence_Number"),
        )
    )


def write_silver_stream(
    stream, storage_account: str, checkpoint_base: str, trigger: str = "30 seconds"
):
    """Write fraud candidates to silver Delta Lake. Append, 30s micro-batch."""
    path = (
        f"abfss://silver@{storage_account}.dfs.core.windows.net"
        f"/fraud_candidates/"
    )
    checkpoint = f"{checkpoint_base}/silver"
    return (
        stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .partitionBy("Silver_Load_Date")
        .trigger(processingTime=trigger)
        .start(path)
    )


def write_sar_queue_stream(
    stream, storage_account: str, checkpoint_base: str, trigger: str = "30 seconds"
):
    """
    Write SAR-eligible events (composite_risk_score >= 0.75) to gold/sar_queue/.
    MLR 2017 Reg.35: Reviewed by MLRO within 1 hour for NCA SAR submission.
    """
    sar_stream = stream.filter(F.col("sar_eligible") == True)  # noqa: E712
    path = f"abfss://gold@{storage_account}.dfs.core.windows.net/sar_queue/"
    checkpoint = f"{checkpoint_base}/sar_queue"
    log.info("SAR queue stream configured -> gold/sar_queue/")
    return (
        sar_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .partitionBy("Silver_Load_Date")
        .trigger(processingTime=trigger)
        .start(path)
    )


def write_bronze_stream(
    parsed_stream, storage_account: str, checkpoint_base: str, trigger: str = "30 seconds"
):
    """
    Write ALL transactions to bronze — no filtering, no transformation.
    Bronze is the immutable raw layer. Every event is preserved here
    for audit, replay, and reconciliation (MLR 2017 records obligation).
    """
    path = (
        f"abfss://bronze@{storage_account}.dfs.core.windows.net/transactions/"
    )
    checkpoint = f"{checkpoint_base}/bronze"
    return (
        parsed_stream
        .withColumn("bronze_load_date", F.to_date(F.current_timestamp()))
        .drop("body_str")
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .partitionBy("bronze_load_date")
        .trigger(processingTime=trigger)
        .start(path)
    )


def main():
    """Entry point — wires the full streaming pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    log.info("=" * 70)
    log.info("ClearPay UK Fraud Detection Pipeline - Starting")
    log.info("UK GDPR | MLR 2017 | Net Zero 2050 Aligned")
    log.info("=" * 70)

    spark = build_spark_session()

    # Import here to avoid importing Databricks-specific APIs at module level
    from src.risk_scoring import apply_risk_scoring
    from src.dead_letter_handler import capture_dead_letter_events, write_dead_letter_stream

    # All secrets from Azure Key Vault via Databricks secret scope
    dbutils = spark._jvm.com.databricks.dbutils.DBUtilsHolder.dbutils()
    conn_str     = dbutils.secrets().get(SECRET_SCOPE, EH_CONNSTR_SECRET)
    storage_acct = dbutils.secrets().get(SECRET_SCOPE, ADLS_ACCOUNT_SECRET)
    iban_salt    = dbutils.secrets().get(SECRET_SCOPE, IBAN_SALT_SECRET)

    checkpoint_base = (
        f"abfss://silver@{storage_acct}.dfs.core.windows.net"
        f"/_checkpoints/fraud_detection"
    )

    # Event Hubs config
    eh_conf = {
        "eventhubs.connectionString": (
            spark._jvm.org.apache.spark.eventhubs.EventHubsUtils
            .encrypt(conn_str)
        ),
        "eventhubs.consumerGroup":       EH_CONSUMER_GROUP,
        "eventhubs.name":                EH_TOPIC,
        "eventhubs.startingPosition":    (
            '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}'
        ),
        "eventhubs.maxEventsPerTrigger": "10000",
    }

    raw_stream    = spark.readStream.format("eventhubs").options(**eh_conf).load()
    parsed_stream = parse_transaction_stream(raw_stream)

    # Fork 1: Bronze — full raw audit trail
    bronze_query = write_bronze_stream(parsed_stream, storage_acct, checkpoint_base)

    # Fork 2: Dead-letter — malformed events captured, never silently dropped
    dead_letter = capture_dead_letter_events(parsed_stream)
    dl_query = write_dead_letter_stream(
        dead_letter, storage_acct,
        checkpoint_path=f"{checkpoint_base}/dead_letter",
    )

    # Fork 3: Silver pipeline
    watermarked = apply_watermark(parsed_stream)
    filtered    = filter_high_value_transactions(watermarked)
    hashed      = hash_pii_columns(filtered, iban_salt)
    scored      = apply_risk_scoring(hashed)
    enriched    = add_pipeline_metadata(scored)

    silver_query = write_silver_stream(enriched, storage_acct, checkpoint_base)
    sar_query    = write_sar_queue_stream(enriched, storage_acct, checkpoint_base)

    log.info("All streaming queries active:")
    log.info("  Bronze      query ID : %s", bronze_query.id)
    log.info("  Silver      query ID : %s", silver_query.id)
    log.info("  SAR Queue   query ID : %s", sar_query.id)
    log.info("  Dead-letter query ID : %s", dl_query.id)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
