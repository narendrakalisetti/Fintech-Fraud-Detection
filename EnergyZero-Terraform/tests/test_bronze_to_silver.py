"""
tests/test_bronze_to_silver.py
Unit tests for bronze→silver transformation functions.
Uses PySpark local mode — no Azure connection required.

Run: pytest tests/ -v --cov=notebooks --cov-report=term-missing
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from datetime import datetime

# Add notebooks to path
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from notebooks.bronze_to_silver_functions import (
    hash_customer_id,
    validate_and_cleanse,
    add_metadata,
    drop_pii_columns,
    select_silver_columns,
)


@pytest.fixture(scope="session")
def spark():
    """Local Spark session for unit tests — no Azure required."""
    return (
        SparkSession.builder
        .appName("EnergyZero-UnitTests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")   # Speed up tests
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def sample_bronze_df(spark):
    """Create a sample bronze DataFrame matching Ofgem CSV schema."""
    schema = StructType([
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
    data = [
        ("R001", "CUST-001", "MTR-001", datetime(2024, 1, 15, 8, 0), 12.5, 230.0, "SW",  "GREEN",     "OFGEM"),
        ("R002", "CUST-002", "MTR-002", datetime(2024, 1, 15, 9, 0), 8.3,  229.5, "NW",  "STANDARD",  "OFGEM"),
        ("R003", "CUST-003", None,      datetime(2024, 1, 15, 10, 0), 0.0, 231.0, "SE",  "renewable", "OFGEM"),  # lower case tariff
        ("R001", "CUST-001", "MTR-001", datetime(2024, 1, 15, 8, 0), 12.5, 230.0, "SW",  "GREEN",     "OFGEM"),  # duplicate
        ("R004", None,       "MTR-004", datetime(2024, 1, 15, 11, 0), 5.0, 228.0, "NE",  "STANDARD",  "OFGEM"),  # null customer_id
        ("R005", "CUST-005", "MTR-005", datetime(2024, 1, 15, 12, 0), -3.0, 230.0, "YH", "EV",        "OFGEM"),  # negative kwh
        ("R006", "CUST-006", "MTR-006", datetime(2024, 1, 15, 13, 0), 7.1, 270.0, "EM",  "STANDARD",  "OFGEM"),  # over-voltage
    ]
    return spark.createDataFrame(data, schema)


class TestHashCustomerId:
    def test_produces_customer_id_hash_column(self, spark, sample_bronze_df):
        df = sample_bronze_df.filter(F.col("customer_id").isNotNull())
        result = hash_customer_id(df, salt="test-salt-123")
        assert "customer_id_hash" in result.columns

    def test_removes_raw_customer_id(self, spark, sample_bronze_df):
        df = sample_bronze_df.filter(F.col("customer_id").isNotNull())
        result = hash_customer_id(df, salt="test-salt-123")
        assert "customer_id" not in result.columns

    def test_hash_is_deterministic(self, spark, sample_bronze_df):
        df = sample_bronze_df.filter(F.col("customer_id") == "CUST-001")
        r1 = hash_customer_id(df, salt="salt").select("customer_id_hash").first()[0]
        r2 = hash_customer_id(df, salt="salt").select("customer_id_hash").first()[0]
        assert r1 == r2

    def test_different_salts_produce_different_hashes(self, spark, sample_bronze_df):
        df = sample_bronze_df.filter(F.col("customer_id") == "CUST-001")
        h1 = hash_customer_id(df, salt="salt1").select("customer_id_hash").first()[0]
        h2 = hash_customer_id(df, salt="salt2").select("customer_id_hash").first()[0]
        assert h1 != h2

    def test_hash_is_64_chars_sha256(self, spark, sample_bronze_df):
        df = sample_bronze_df.filter(F.col("customer_id") == "CUST-001")
        h = hash_customer_id(df, salt="salt").select("customer_id_hash").first()[0]
        assert len(h) == 64


class TestValidateAndCleanse:
    def test_removes_duplicates(self, spark, sample_bronze_df):
        # R001 appears twice in sample data
        result = validate_and_cleanse(sample_bronze_df)
        count_r001 = result.filter(F.col("reading_id") == "R001").count()
        assert count_r001 == 1

    def test_removes_negative_kwh(self, spark, sample_bronze_df):
        result = validate_and_cleanse(sample_bronze_df)
        neg = result.filter(F.col("kwh_consumed") < 0).count()
        assert neg == 0

    def test_removes_over_voltage(self, spark, sample_bronze_df):
        # R006 has voltage 270 — above UK grid tolerance of 253
        result = validate_and_cleanse(sample_bronze_df)
        assert result.filter(F.col("reading_id") == "R006").count() == 0

    def test_upcases_tariff_type(self, spark, sample_bronze_df):
        result = validate_and_cleanse(sample_bronze_df)
        lowercase = result.filter(F.col("tariff_type") == F.lower(F.col("tariff_type"))).count()
        assert lowercase == 0  # all should be uppercased

    def test_upcases_region_code(self, spark, sample_bronze_df):
        result = validate_and_cleanse(sample_bronze_df)
        for row in result.select("region_code").collect():
            assert row.region_code == row.region_code.upper()

    def test_keeps_valid_rows(self, spark, sample_bronze_df):
        result = validate_and_cleanse(sample_bronze_df)
        assert result.count() >= 2  # R001 and R002 should always survive


class TestAddMetadata:
    def test_adds_ingest_date_column(self, spark, sample_bronze_df):
        result = add_metadata(sample_bronze_df, "RUN-001")
        assert "ingest_date" in result.columns

    def test_adds_pipeline_run_id(self, spark, sample_bronze_df):
        result = add_metadata(sample_bronze_df, "RUN-XYZ")
        val = result.select("pipeline_run_id").first()[0]
        assert val == "RUN-XYZ"

    def test_ingest_date_format(self, spark, sample_bronze_df):
        result = add_metadata(sample_bronze_df, "RUN-001")
        date_val = result.select("ingest_date").first()[0]
        # Should be YYYY-MM-DD
        import re
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", date_val)


class TestDropPiiColumns:
    def test_drops_email_if_present(self, spark, sample_bronze_df):
        df_with_email = sample_bronze_df.withColumn("email", F.lit("test@test.com"))
        result = drop_pii_columns(df_with_email)
        assert "email" not in result.columns

    def test_no_error_if_no_pii_columns(self, spark, sample_bronze_df):
        # Should not raise — just returns df unchanged
        result = drop_pii_columns(sample_bronze_df)
        assert result.count() == sample_bronze_df.count()

    def test_customer_id_dropped(self, spark, sample_bronze_df):
        result = drop_pii_columns(sample_bronze_df)
        assert "customer_id" not in result.columns
