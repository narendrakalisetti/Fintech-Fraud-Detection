"""
Notebook: 01_bronze_to_silver.py
Purpose : Cleanse Ofgem CSV data, hash customer PII, validate schema,
          write Delta Lake silver layer.
Author  : Narendra Kalisetti
Compliance: UK GDPR Art. 25 (Privacy by Design), Art. 5 (Data Minimisation)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType
)
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema definition — explicit schema enforcement (rejects malformed rows)
# ---------------------------------------------------------------------------
OFGEM_SCHEMA = StructType([
    StructField("reading_id",       StringType(),    False),
    StructField("customer_id",      StringType(),    False),   # PII — hashed at silver
    StructField("meter_serial",     StringType(),    True),
    StructField("reading_datetime", TimestampType(), False),
    StructField("kwh_consumed",     DoubleType(),    False),
    StructField("voltage",          DoubleType(),    True),
    StructField("region_code",      StringType(),    False),
    StructField("tariff_type",      StringType(),    True),
    StructField("data_source",      StringType(),    True),
])

SILVER_SCHEMA = StructType([
    StructField("reading_id",         StringType(),    False),
    StructField("customer_id_hash",   StringType(),    False),   # SHA-256 hashed
    StructField("meter_serial",       StringType(),    True),
    StructField("reading_datetime",   TimestampType(), False),
    StructField("kwh_consumed",       DoubleType(),    False),
    StructField("voltage",            DoubleType(),    True),
    StructField("region_code",        StringType(),    False),
    StructField("tariff_type",        StringType(),    True),
    StructField("ingest_date",        StringType(),    False),
    StructField("pipeline_run_id",    StringType(),    True),
])


# ---------------------------------------------------------------------------
# Transformation functions (pure — unit testable without Spark context)
# ---------------------------------------------------------------------------

def hash_customer_id(df: DataFrame, salt: str) -> DataFrame:
    """
    Hash customer_id using SHA-256 + salt.
    GDPR Art. 25: Raw PII never written to silver or gold layers.
    """
    return df.withColumn(
        "customer_id_hash",
        F.sha2(F.concat(F.col("customer_id"), F.lit(salt)), 256)
    ).drop("customer_id")


def validate_and_cleanse(df: DataFrame) -> DataFrame:
    """
    Drop nulls on mandatory fields, remove duplicates, cast types.
    """
    return (
        df
        .dropna(subset=["reading_id", "reading_datetime", "kwh_consumed", "region_code"])
        .dropDuplicates(["reading_id"])
        .filter(F.col("kwh_consumed") >= 0)            # negative readings are invalid
        .filter(F.col("voltage").isNull() | (F.col("voltage").between(200, 260)))  # UK grid: 230V ±10%
        .withColumn("tariff_type", F.upper(F.trim(F.col("tariff_type"))))
        .withColumn("region_code", F.upper(F.trim(F.col("region_code"))))
    )


def add_metadata(df: DataFrame, pipeline_run_id: str) -> DataFrame:
    """Add audit columns required for GDPR Art. 30 records of processing."""
    return (
        df
        .withColumn("ingest_date",     F.date_format(F.current_timestamp(), "yyyy-MM-dd"))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    )


def drop_pii_columns(df: DataFrame) -> DataFrame:
    """
    Explicitly drop any remaining PII columns before silver write.
    Belt-and-braces approach: even if hash_customer_id missed something.
    """
    pii_columns = ["customer_id", "email", "phone", "address", "postcode"]
    existing_pii = [c for c in pii_columns if c in df.columns]
    if existing_pii:
        logger.warning(f"Dropping PII columns before silver write: {existing_pii}")
        df = df.drop(*existing_pii)
    return df


def select_silver_columns(df: DataFrame) -> DataFrame:
    """Select only the approved silver schema columns."""
    silver_cols = [f.name for f in SILVER_SCHEMA.fields]
    existing = [c for c in silver_cols if c in df.columns]
    return df.select(*existing)


# ---------------------------------------------------------------------------
# Delta Lake upsert (MERGE — idempotent, handles re-runs)
# ---------------------------------------------------------------------------

def upsert_to_silver(spark: SparkSession, df: DataFrame, silver_path: str) -> None:
    """
    Upsert to Delta silver using MERGE on reading_id.
    Idempotent — safe to re-run on pipeline failure.
    Also runs OPTIMIZE + ZORDER for Power BI DirectQuery performance.
    """
    if DeltaTable.isDeltaTable(spark, silver_path):
        delta_table = DeltaTable.forPath(spark, silver_path)
        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                "target.reading_id = source.reading_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # First run — create the table
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("ingest_date", "region_code")
            .option("overwriteSchema", "true")
            .save(silver_path)
        )

    # Delta maintenance — prevents small file problem
    spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (reading_datetime, region_code)")
    spark.sql(f"VACUUM delta.`{silver_path}` RETAIN 168 HOURS")  # 7 days


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_bronze_to_silver(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    pii_salt: str,
    pipeline_run_id: str
) -> dict:
    """
    Full bronze → silver transformation pipeline.
    Returns metrics dict for monitoring.
    """
    logger.info(f"Starting bronze→silver | run_id={pipeline_run_id}")

    # 1. Read bronze with explicit schema
    df_raw = (
        spark.read
        .format("csv")
        .schema(OFGEM_SCHEMA)
        .option("header", "true")
        .option("mode", "PERMISSIVE")           # bad rows go to _corrupt_record
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(bronze_path)
    )

    bronze_count = df_raw.count()
    corrupt_count = df_raw.filter(F.col("_corrupt_record").isNotNull()).count()
    logger.info(f"Bronze rows read: {bronze_count} | Corrupt: {corrupt_count}")

    # Drop corrupt rows (they stay in bronze for audit)
    df_clean = df_raw.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    # 2. Validate and cleanse
    df_cleansed = validate_and_cleanse(df_clean)
    cleansed_count = df_cleansed.count()
    dropped_count = bronze_count - corrupt_count - cleansed_count
    logger.info(f"After cleanse: {cleansed_count} rows | Dropped: {dropped_count}")

    # 3. Hash PII (GDPR Art. 25)
    df_hashed = hash_customer_id(df_cleansed, salt=pii_salt)

    # 4. Add metadata
    df_with_meta = add_metadata(df_hashed, pipeline_run_id)

    # 5. Belt-and-braces PII check
    df_safe = drop_pii_columns(df_with_meta)

    # 6. Select silver columns
    df_silver = select_silver_columns(df_safe)

    # 7. Upsert to Delta silver
    upsert_to_silver(spark, df_silver, silver_path)
    logger.info(f"Silver upsert complete: {silver_path}")

    metrics = {
        "bronze_count":   bronze_count,
        "corrupt_count":  corrupt_count,
        "cleansed_count": cleansed_count,
        "dropped_count":  dropped_count,
        "silver_written": cleansed_count,
        "pipeline_run_id": pipeline_run_id,
    }
    return metrics


# ---------------------------------------------------------------------------
# Databricks entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uuid

    spark = SparkSession.builder.appName("EnergyZero-BronzeToSilver").getOrCreate()
    spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Retrieve salt from Databricks secret scope (Key Vault-backed)
    pii_salt = dbutils.secrets.get(scope="energyzero-kv", key="pii-hash-salt")  # noqa: F821

    BRONZE_PATH = "abfss://bronze@sauksenergyzeroprod.dfs.core.windows.net/ofgem/"
    SILVER_PATH = "abfss://silver@sauksenergyzeroprod.dfs.core.windows.net/meter_readings/"

    metrics = run_bronze_to_silver(
        spark=spark,
        bronze_path=BRONZE_PATH,
        silver_path=SILVER_PATH,
        pii_salt=pii_salt,
        pipeline_run_id=str(uuid.uuid4()),
    )
    print(f"Pipeline metrics: {metrics}")
