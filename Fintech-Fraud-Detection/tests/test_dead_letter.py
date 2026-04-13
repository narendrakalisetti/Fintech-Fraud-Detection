"""
tests/test_dead_letter.py
Unit tests for dead-letter handler — malformed event capture and classification.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType
from datetime import datetime
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from dead_letter_handler import classify_failure
from fraud_detection_stream import TRANSACTION_SCHEMA


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("DeadLetter-Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def malformed_df(spark):
    data = [
        # missing transaction_id
        (None, "ACC-001", None, None, 500.0, "GBP", "CARD",
         "RETAIL", "GB", datetime(2024, 1, 1, 10, 0), "D1", "GB", False),
        # missing amount
        ("TXN-B", "ACC-002", None, None, None, "GBP", "CARD",
         "RETAIL", "GB", datetime(2024, 1, 1, 10, 1), "D2", "GB", False),
        # missing account_id
        ("TXN-C", None, None, None, 100.0, "GBP", "BACS",
         "GROCERY", "GB", datetime(2024, 1, 1, 10, 2), "D3", "GB", False),
    ]
    return spark.createDataFrame(data, TRANSACTION_SCHEMA)


class TestClassifyFailure:
    def test_missing_transaction_id_classified(self, spark, malformed_df):
        result = classify_failure(malformed_df)
        row = result.filter(F.col("account_id") == "ACC-001") \
                    .select("failure_category").first()[0]
        assert row == "MISSING_TRANSACTION_ID"

    def test_missing_amount_classified(self, spark, malformed_df):
        result = classify_failure(malformed_df)
        row = result.filter(F.col("transaction_id") == "TXN-B") \
                    .select("failure_category").first()[0]
        assert row == "MISSING_AMOUNT"

    def test_missing_account_id_classified(self, spark, malformed_df):
        result = classify_failure(malformed_df)
        row = result.filter(F.col("transaction_id") == "TXN-C") \
                    .select("failure_category").first()[0]
        assert row == "MISSING_ACCOUNT_ID"

    def test_captured_at_added(self, spark, malformed_df):
        result = classify_failure(malformed_df)
        assert "captured_at" in result.columns

    def test_review_status_is_pending(self, spark, malformed_df):
        result = classify_failure(malformed_df)
        statuses = {r.review_status for r in result.select("review_status").collect()}
        assert statuses == {"PENDING"}

    def test_all_rows_classified(self, spark, malformed_df):
        result = classify_failure(malformed_df)
        null_category = result.filter(F.col("failure_category").isNull()).count()
        assert null_category == 0
