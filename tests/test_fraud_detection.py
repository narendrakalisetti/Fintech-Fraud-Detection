"""
Unit tests for Fintech Fraud Detection Pipeline.
Uses pytest + pyspark local session (no Event Hubs required).
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime

from src.fraud_detection_stream import (
    filter_high_value_transactions,
    hash_pii_columns,
    add_pipeline_metadata,
    HIGH_VALUE_THRESHOLD_GBP,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("Fintech-FraudDetection-Tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


SAMPLE_SCHEMA = StructType([
    StructField("Transaction_ID",           StringType(),    False),
    StructField("Event_Timestamp",          TimestampType(), False),
    StructField("Customer_Account_IBAN",    StringType(),    False),
    StructField("Transaction_Amount_GBP",   DoubleType(),    False),
    StructField("Merchant_ID",              StringType(),    True),
])

SAMPLE_ROWS = [
    ("TXN001", datetime(2024, 6, 1, 10, 0, 0), "GB29NWBK60161331926819", 4_999.99, "M001"),
    ("TXN002", datetime(2024, 6, 1, 10, 1, 0), "GB29NWBK60161331926820", 5_000.01, "M002"),
    ("TXN003", datetime(2024, 6, 1, 10, 2, 0), "GB29NWBK60161331926821", 10_000.00, "M003"),
    ("TXN004", datetime(2024, 6, 1, 10, 3, 0), "GB29NWBK60161331926822", 100.00,   "M004"),
]


def make_df(spark):
    return spark.createDataFrame(SAMPLE_ROWS, schema=SAMPLE_SCHEMA)


class TestHighValueFilter:
    def test_filters_below_threshold(self, spark):
        df = make_df(spark)
        result = filter_high_value_transactions(df)
        assert result.count() == 2  # TXN002 and TXN003

    def test_threshold_boundary_excluded(self, spark):
        df = make_df(spark)
        result = filter_high_value_transactions(df)
        ids = [r.Transaction_ID for r in result.select("Transaction_ID").collect()]
        assert "TXN001" not in ids  # £4,999.99 – below threshold
        assert "TXN004" not in ids  # £100 – well below threshold

    def test_threshold_boundary_included(self, spark):
        df = make_df(spark)
        result = filter_high_value_transactions(df)
        ids = [r.Transaction_ID for r in result.select("Transaction_ID").collect()]
        assert "TXN002" in ids   # £5,000.01 – above threshold
        assert "TXN003" in ids   # £10,000.00 – above threshold

    def test_threshold_value_is_correct(self):
        assert HIGH_VALUE_THRESHOLD_GBP == 5_000.0


class TestGDPRHashing:
    def test_raw_iban_column_removed(self, spark):
        df = make_df(spark)
        result = hash_pii_columns(df)
        assert "Customer_Account_IBAN" not in result.columns, \
            "GDPR violation: raw IBAN column must be dropped"

    def test_hashed_column_present(self, spark):
        df = make_df(spark)
        result = hash_pii_columns(df)
        assert "Customer_Account_IBAN_Hashed" in result.columns

    def test_hash_is_sha256_hex(self, spark):
        df = make_df(spark)
        result = hash_pii_columns(df)
        hashes = [r.Customer_Account_IBAN_Hashed for r in result.select("Customer_Account_IBAN_Hashed").collect()]
        for h in hashes:
            assert len(h) == 64, f"Expected 64-char SHA-256 hex digest, got {len(h)}"
            assert all(c in "0123456789abcdef" for c in h), "Hash contains non-hex characters"

    def test_identical_ibans_produce_same_hash(self, spark):
        rows = [
            ("A", datetime(2024, 1, 1), "GB29NWBK60161331926819", 6000.0, "M1"),
            ("B", datetime(2024, 1, 1), "GB29NWBK60161331926819", 7000.0, "M2"),
        ]
        df = spark.createDataFrame(rows, schema=SAMPLE_SCHEMA)
        result = hash_pii_columns(df)
        hashes = [r.Customer_Account_IBAN_Hashed for r in result.select("Customer_Account_IBAN_Hashed").collect()]
        assert hashes[0] == hashes[1], "Same IBAN should produce same hash (deterministic)"

    def test_different_ibans_produce_different_hashes(self, spark):
        df = make_df(spark)
        result = hash_pii_columns(df)
        hashes = [r.Customer_Account_IBAN_Hashed for r in result.select("Customer_Account_IBAN_Hashed").collect()]
        assert len(set(hashes)) == len(hashes), "Different IBANs must produce different hashes"


class TestPipelineMetadata:
    def test_metadata_columns_present(self, spark):
        df = make_df(spark)
        result = add_pipeline_metadata(df)
        for col in ["Pipeline_Name", "GDPR_PII_Hashed", "Data_Classification", "Silver_Load_Date"]:
            assert col in result.columns

    def test_gdpr_flag_is_true(self, spark):
        df = make_df(spark)
        result = add_pipeline_metadata(df)
        flags = [r.GDPR_PII_Hashed for r in result.select("GDPR_PII_Hashed").collect()]
        assert all(flags), "GDPR_PII_Hashed must be True for all records"

    def test_classification_label(self, spark):
        df = make_df(spark)
        result = add_pipeline_metadata(df)
        labels = [r.Data_Classification for r in result.select("Data_Classification").collect()]
        assert all(l == "CONFIDENTIAL" for l in labels)
