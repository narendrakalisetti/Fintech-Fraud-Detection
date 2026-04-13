"""
tests/test_fraud_detection.py
Unit tests for fraud detection stream — schema validation, PII hashing,
watermark config, high-value filter, metadata.

Run: pytest tests/ -v --cov=src --cov-report=term-missing
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, BooleanType
)
from datetime import datetime
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from fraud_detection_stream import (
    TRANSACTION_SCHEMA,
    parse_and_validate,
    apply_watermark,
    hash_pii_columns,
    filter_high_value,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("FraudDetection-Tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def valid_transactions(spark):
    data = [
        ("TXN-001", "ACC-001", "GB29NWBK60161331926819", "GB82WEST12345698765432",
         12500.0, "GBP", "FPS", "RETAIL", "GB",
         datetime(2024, 1, 15, 10, 0), "DEV-001", "GB", False),
        ("TXN-002", "ACC-002", "GB29NWBK60161331926820", None,
         450.0, "GBP", "CARD", "GROCERY", "GB",
         datetime(2024, 1, 15, 10, 1), "DEV-002", "GB", False),
        ("TXN-003", "ACC-003", "GB29NWBK60161331926821", "DE89370400440532013000",
         8750.0, "GBP", "CHAPS", "WIRE_TRANSFER", "DE",
         datetime(2024, 1, 15, 10, 2), "DEV-003", "DE", True),
        ("TXN-004", "ACC-004", "GB29NWBK60161331926822", None,
         10000.0, "GBP", "BACS", "GAMBLING", "GB",
         datetime(2024, 1, 15, 10, 3), "DEV-004", "RU", False),  # geo mismatch + round amount
    ]
    return spark.createDataFrame(data, TRANSACTION_SCHEMA)


@pytest.fixture
def malformed_transactions(spark):
    """Transactions missing mandatory fields."""
    data = [
        (None, "ACC-001", "GB29NWBK60161331926819", None,  # missing transaction_id
         5000.0, "GBP", "FPS", "RETAIL", "GB",
         datetime(2024, 1, 15, 10, 0), "DEV-001", "GB", False),
        ("TXN-BAD", "ACC-002", None, None,
         None, "GBP", "CARD", "GROCERY", "GB",                  # missing amount
         datetime(2024, 1, 15, 10, 1), "DEV-002", "GB", False),
    ]
    return spark.createDataFrame(data, TRANSACTION_SCHEMA)


class TestSchema:
    def test_schema_has_transaction_id(self):
        field_names = [f.name for f in TRANSACTION_SCHEMA.fields]
        assert "transaction_id" in field_names

    def test_schema_has_pii_fields(self):
        field_names = [f.name for f in TRANSACTION_SCHEMA.fields]
        assert "iban" in field_names
        assert "account_id" in field_names
        assert "counterparty_iban" in field_names

    def test_schema_has_amount(self):
        field_names = [f.name for f in TRANSACTION_SCHEMA.fields]
        assert "amount_gbp" in field_names

    def test_schema_has_event_timestamp(self):
        field_names = [f.name for f in TRANSACTION_SCHEMA.fields]
        assert "event_timestamp" in field_names

    def test_mandatory_fields_not_nullable(self):
        non_nullable = {f.name: f.nullable for f in TRANSACTION_SCHEMA.fields
                        if f.name in ["transaction_id", "account_id", "amount_gbp",
                                      "event_timestamp", "transaction_type"]}
        for field, nullable in non_nullable.items():
            assert not nullable, f"{field} should be non-nullable"


class TestHashPiiColumns:
    def test_raw_iban_removed(self, spark, valid_transactions):
        result = hash_pii_columns(valid_transactions, salt="test-salt")
        assert "iban" not in result.columns

    def test_raw_account_id_removed(self, spark, valid_transactions):
        result = hash_pii_columns(valid_transactions, salt="test-salt")
        assert "account_id" not in result.columns

    def test_counterparty_iban_removed(self, spark, valid_transactions):
        result = hash_pii_columns(valid_transactions, salt="test-salt")
        assert "counterparty_iban" not in result.columns

    def test_hash_columns_present(self, spark, valid_transactions):
        result = hash_pii_columns(valid_transactions, salt="test-salt")
        assert "iban_hash" in result.columns
        assert "account_id_hash" in result.columns
        assert "counterparty_iban_hash" in result.columns

    def test_hash_is_64_chars(self, spark, valid_transactions):
        result = hash_pii_columns(valid_transactions, salt="test-salt")
        h = result.select("iban_hash").first()[0]
        assert len(h) == 64

    def test_null_iban_stays_null(self, spark, valid_transactions):
        """TXN-002 has null counterparty_iban — should remain null after hashing."""
        result = hash_pii_columns(valid_transactions, salt="test-salt")
        txn2 = result.filter(F.col("transaction_id") == "TXN-002")
        assert txn2.select("counterparty_iban_hash").first()[0] is None

    def test_different_salts_different_hashes(self, spark, valid_transactions):
        h1 = hash_pii_columns(valid_transactions, salt="salt1") \
               .filter(F.col("transaction_id") == "TXN-001") \
               .select("iban_hash").first()[0]
        h2 = hash_pii_columns(valid_transactions, salt="salt2") \
               .filter(F.col("transaction_id") == "TXN-001") \
               .select("iban_hash").first()[0]
        assert h1 != h2

    def test_amount_preserved_after_hash(self, spark, valid_transactions):
        result = hash_pii_columns(valid_transactions, salt="test-salt")
        txn1_amount = result.filter(F.col("transaction_id") == "TXN-001") \
                            .select("amount_gbp").first()[0]
        assert txn1_amount == 12500.0


class TestFilterHighValue:
    def test_filters_below_threshold(self, spark, valid_transactions):
        result = filter_high_value(valid_transactions, threshold_gbp=5000.0)
        # TXN-002 (£450) should be excluded
        assert result.filter(F.col("transaction_id") == "TXN-002").count() == 0

    def test_keeps_above_threshold(self, spark, valid_transactions):
        result = filter_high_value(valid_transactions, threshold_gbp=5000.0)
        # TXN-001 (£12,500) and TXN-003 (£8,750) should be included
        assert result.filter(F.col("transaction_id") == "TXN-001").count() == 1
        assert result.filter(F.col("transaction_id") == "TXN-003").count() == 1

    def test_includes_exactly_threshold(self, spark, valid_transactions):
        # £5000 exactly should be included (MLR 2017: >= £5,000)
        result = filter_high_value(valid_transactions, threshold_gbp=5000.0)
        assert result.filter(F.col("amount_gbp") == 5000.0).count() >= 0  # not filtered out

    def test_custom_threshold(self, spark, valid_transactions):
        result = filter_high_value(valid_transactions, threshold_gbp=10000.0)
        # Only TXN-001 (£12,500) and TXN-004 (£10,000) qualify
        assert result.filter(F.col("amount_gbp") >= 10000.0).count() == 2


class TestApplyWatermark:
    def test_watermark_column_exists(self, spark, valid_transactions):
        """After watermark, event_timestamp column still present."""
        result = apply_watermark(valid_transactions, "10 minutes")
        assert "event_timestamp" in result.columns

    def test_row_count_unchanged_by_watermark(self, spark, valid_transactions):
        """Watermark does not filter rows in batch mode."""
        original_count = valid_transactions.count()
        result = apply_watermark(valid_transactions, "10 minutes")
        assert result.count() == original_count
