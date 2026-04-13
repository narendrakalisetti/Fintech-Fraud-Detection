"""
tests/test_integration.py
Integration tests — full pipeline: parse → validate → hash PII → score → route SAR.
Uses only local PySpark (no Azure required).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from fraud_detection_stream import (
    TRANSACTION_SCHEMA, hash_pii_columns, filter_high_value, apply_watermark
)
from risk_scoring import apply_risk_scoring, route_sar_queue
from dead_letter_handler import classify_failure


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("Integration-Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def mixed_transactions(spark):
    """Mix of valid, SAR-eligible, and borderline transactions."""
    data = [
        ("TXN-A", "ACC-A", "GB29NWBK60161331926819", None,
         49.99, "GBP", "CARD", "GROCERY", "GB",
         datetime(2024, 1, 15, 10, 0), "D1", "GB", False),
        ("TXN-B", "ACC-B", "GB29NWBK60161331926820", "KP99NWBK60161331926820",
         30000.0, "GBP", "CHAPS", "WIRE_TRANSFER", "KP",
         datetime(2024, 1, 15, 10, 1), "D2", "KP", True),
        ("TXN-C", "ACC-C", "GB29NWBK60161331926821", None,
         7500.0, "GBP", "FPS", "CRYPTO", "GB",
         datetime(2024, 1, 15, 10, 2), "D3", "RU", False),
    ]
    return spark.createDataFrame(data, TRANSACTION_SCHEMA)


class TestFullPipeline:
    def test_pii_removed_before_scoring(self, spark, mixed_transactions):
        hashed = hash_pii_columns(mixed_transactions, salt="integration-salt")
        assert "iban" not in hashed.columns
        assert "account_id" not in hashed.columns
        assert "counterparty_iban" not in hashed.columns

    def test_sar_eligible_detected_in_full_pipeline(self, spark, mixed_transactions):
        hashed = hash_pii_columns(mixed_transactions, salt="integration-salt")
        scored = apply_risk_scoring(hashed)
        sar_df, _ = route_sar_queue(scored)
        sar_ids = {r.transaction_id for r in sar_df.select("transaction_id").collect()}
        # TXN-B: KP country + £30,000 + WIRE_TRANSFER = SAR eligible
        assert "TXN-B" in sar_ids

    def test_low_risk_not_in_sar(self, spark, mixed_transactions):
        hashed = hash_pii_columns(mixed_transactions, salt="integration-salt")
        scored = apply_risk_scoring(hashed)
        sar_df, _ = route_sar_queue(scored)
        sar_ids = {r.transaction_id for r in sar_df.select("transaction_id").collect()}
        assert "TXN-A" not in sar_ids

    def test_all_transactions_have_risk_score(self, spark, mixed_transactions):
        hashed = hash_pii_columns(mixed_transactions, salt="integration-salt")
        scored = apply_risk_scoring(hashed)
        null_scores = scored.filter(F.col("risk_score").isNull()).count()
        assert null_scores == 0

    def test_dead_letter_classification_on_malformed(self, spark):
        data = [
            (None, "ACC-X", None, None, 100.0, "GBP", "CARD",
             "RETAIL", "GB", datetime(2024, 1, 1, 0, 0), "D", "GB", False),
        ]
        df = spark.createDataFrame(data, TRANSACTION_SCHEMA)
        result = classify_failure(df)
        assert result.select("failure_category").first()[0] == "MISSING_TRANSACTION_ID"

    def test_hash_is_consistent_across_pipeline_runs(self, spark, mixed_transactions):
        """Same input + same salt = same hashes (deterministic)."""
        r1 = hash_pii_columns(mixed_transactions, salt="fixed-salt") \
               .filter(F.col("transaction_id") == "TXN-A") \
               .select("iban_hash").first()[0]
        r2 = hash_pii_columns(mixed_transactions, salt="fixed-salt") \
               .filter(F.col("transaction_id") == "TXN-A") \
               .select("iban_hash").first()[0]
        assert r1 == r2
