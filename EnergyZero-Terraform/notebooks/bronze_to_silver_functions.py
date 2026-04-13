"""
notebooks/bronze_to_silver_functions.py
Pure transformation functions extracted from 01_bronze_to_silver.py.
These have no Databricks runtime dependencies (no dbutils, no SparkSession init)
so they can be imported and unit-tested locally with PySpark local mode.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType,
)
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)

OFGEM_SCHEMA = StructType([
    StructField("reading_id",       StringType(),    False),
    StructField("customer_id",      StringType(),    False),
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
    StructField("customer_id_hash",   StringType(),    False),
    StructField("meter_serial",       StringType(),    True),
    StructField("reading_datetime",   TimestampType(), False),
    StructField("kwh_consumed",       DoubleType(),    False),
    StructField("voltage",            DoubleType(),    True),
    StructField("region_code",        StringType(),    False),
    StructField("tariff_type",        StringType(),    True),
    StructField("ingest_date",        StringType(),    False),
    StructField("pipeline_run_id",    StringType(),    True),
])


def hash_customer_id(df: DataFrame, salt: str) -> DataFrame:
    """Hash customer_id using SHA-256+salt. Drops raw customer_id (GDPR Art. 25)."""
    return df.withColumn(
        "customer_id_hash",
        F.sha2(F.concat(F.col("customer_id"), F.lit(salt)), 256)
    ).drop("customer_id")


def validate_and_cleanse(df: DataFrame) -> DataFrame:
    """Drop nulls on mandatory fields, remove duplicates, validate ranges."""
    return (
        df
        .dropna(subset=["reading_id", "reading_datetime", "kwh_consumed", "region_code"])
        .dropDuplicates(["reading_id"])
        .filter(F.col("kwh_consumed") >= 0)
        .filter(F.col("voltage").isNull() | (F.col("voltage").between(200, 260)))
        .withColumn("tariff_type", F.upper(F.trim(F.col("tariff_type"))))
        .withColumn("region_code",  F.upper(F.trim(F.col("region_code"))))
    )


def add_metadata(df: DataFrame, pipeline_run_id: str) -> DataFrame:
    """Add GDPR Art. 30 audit columns."""
    return (
        df
        .withColumn("ingest_date",     F.date_format(F.current_timestamp(), "yyyy-MM-dd"))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    )


def drop_pii_columns(df: DataFrame) -> DataFrame:
    """Belt-and-braces PII drop before silver write."""
    pii_columns = ["customer_id", "email", "phone", "address", "postcode"]
    existing_pii = [c for c in pii_columns if c in df.columns]
    if existing_pii:
        logger.warning(f"Dropping PII columns before silver write: {existing_pii}")
        df = df.drop(*existing_pii)
    return df


def select_silver_columns(df: DataFrame) -> DataFrame:
    """Select only approved silver schema columns."""
    silver_cols = [f.name for f in SILVER_SCHEMA.fields]
    existing = [c for c in silver_cols if c in df.columns]
    return df.select(*existing)


def upsert_to_silver(spark: SparkSession, df: DataFrame, silver_path: str) -> None:
    """Idempotent MERGE into Delta silver + OPTIMIZE + VACUUM."""
    if DeltaTable.isDeltaTable(spark, silver_path):
        delta_table = DeltaTable.forPath(spark, silver_path)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), "target.reading_id = source.reading_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("ingest_date", "region_code")
            .option("overwriteSchema", "true")
            .save(silver_path)
        )
    spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (reading_datetime, region_code)")
    spark.sql(f"VACUUM delta.`{silver_path}` RETAIN 168 HOURS")
