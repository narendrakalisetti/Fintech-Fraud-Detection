"""
tests/test_risk_scoring.py
Unit tests for the rule-based risk scoring engine.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from risk_scoring import (
    score_high_value,
    score_international,
    score_merchant_category,
    score_geo_mismatch,
    score_large_round_amount,
    compute_composite_score,
    route_sar_queue,
    apply_risk_scoring,
    HIGH_RISK_MERCHANT_CATEGORIES,
    HIGH_RISK_COUNTRIES,
)
from fraud_detection_stream import TRANSACTION_SCHEMA


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("RiskScoring-Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def transactions(spark):
    data = [
        # Low risk: small UK card payment
        ("TXN-LOW", "ACC-001", None, None, 49.99, "GBP", "CARD",
         "GROCERY", "GB", datetime(2024, 1, 15, 10, 0), "DEV-001", "GB", False),
        # High value only
        ("TXN-HV", "ACC-002", None, None, 7500.0, "GBP", "FPS",
         "RETAIL", "GB", datetime(2024, 1, 15, 10, 1), "DEV-002", "GB", False),
        # International high-risk country
        ("TXN-INTL", "ACC-003", None, None, 3000.0, "GBP", "CHAPS",
         "WIRE_TRANSFER", "IR", datetime(2024, 1, 15, 10, 2), "DEV-003", "IR", True),
        # Crypto merchant
        ("TXN-CRYPTO", "ACC-004", None, None, 2000.0, "GBP", "CARD",
         "CRYPTO", "GB", datetime(2024, 1, 15, 10, 3), "DEV-004", "GB", False),
        # Round amount geo mismatch
        ("TXN-ROUND", "ACC-005", None, None, 10000.0, "GBP", "BACS",
         "MONEY_TRANSFER", "GB", datetime(2024, 1, 15, 10, 4), "DEV-005", "RU", False),
        # SAR-eligible: high value + international + high-risk merchant
        ("TXN-SAR", "ACC-006", None, None, 25000.0, "GBP", "CHAPS",
         "WIRE_TRANSFER", "KP", datetime(2024, 1, 15, 10, 5), "DEV-006", "KP", True),
    ]
    return spark.createDataFrame(data, TRANSACTION_SCHEMA)


class TestScoreHighValue:
    def test_flags_above_threshold(self, spark, transactions):
        result = score_high_value(transactions)
        hv = result.filter(F.col("transaction_id") == "TXN-HV").select("risk_high_value").first()[0]
        assert hv == 1.0

    def test_no_flag_below_threshold(self, spark, transactions):
        result = score_high_value(transactions)
        low = result.filter(F.col("transaction_id") == "TXN-LOW").select("risk_high_value").first()[0]
        assert low == 0.0


class TestScoreInternational:
    def test_flags_high_risk_country(self, spark, transactions):
        result = score_international(transactions)
        intl = result.filter(F.col("transaction_id") == "TXN-INTL").select("risk_international").first()[0]
        assert intl == 1.0

    def test_no_flag_domestic(self, spark, transactions):
        result = score_international(transactions)
        low = result.filter(F.col("transaction_id") == "TXN-LOW").select("risk_international").first()[0]
        assert low == 0.0


class TestScoreMerchantCategory:
    def test_flags_crypto(self, spark, transactions):
        result = score_merchant_category(transactions)
        crypto = result.filter(F.col("transaction_id") == "TXN-CRYPTO").select("risk_merchant_category").first()[0]
        assert crypto == 1.0

    def test_no_flag_grocery(self, spark, transactions):
        result = score_merchant_category(transactions)
        low = result.filter(F.col("transaction_id") == "TXN-LOW").select("risk_merchant_category").first()[0]
        assert low == 0.0

    def test_all_high_risk_categories_flagged(self, spark):
        for cat in HIGH_RISK_MERCHANT_CATEGORIES:
            data = [("T", "A", None, None, 100.0, "GBP", "CARD",
                     cat, "GB", datetime(2024, 1, 1, 0, 0), "D", "GB", False)]
            df = spark.createDataFrame(data, TRANSACTION_SCHEMA)
            result = score_merchant_category(df)
            score = result.select("risk_merchant_category").first()[0]
            assert score == 1.0, f"Category {cat} should be flagged"


class TestScoreGeoMismatch:
    def test_flags_ip_country_mismatch(self, spark, transactions):
        result = score_geo_mismatch(transactions)
        # TXN-ROUND: country_code=GB, ip_country=RU
        mismatch = result.filter(F.col("transaction_id") == "TXN-ROUND").select("risk_geo_mismatch").first()[0]
        assert mismatch == 1.0

    def test_no_flag_matching_country(self, spark, transactions):
        result = score_geo_mismatch(transactions)
        low = result.filter(F.col("transaction_id") == "TXN-LOW").select("risk_geo_mismatch").first()[0]
        assert low == 0.0


class TestCompositeScore:
    def test_low_risk_transaction_has_low_score(self, spark, transactions):
        result = compute_composite_score(transactions)
        score = result.filter(F.col("transaction_id") == "TXN-LOW").select("risk_score").first()[0]
        assert score < 0.4

    def test_sar_transaction_has_high_score(self, spark, transactions):
        result = compute_composite_score(transactions)
        score = result.filter(F.col("transaction_id") == "TXN-SAR").select("risk_score").first()[0]
        assert score >= 0.6

    def test_risk_label_sar_eligible(self, spark, transactions):
        result = compute_composite_score(transactions)
        label = result.filter(F.col("transaction_id") == "TXN-SAR").select("risk_label").first()[0]
        assert label == "SAR_ELIGIBLE"

    def test_risk_label_low_risk(self, spark, transactions):
        result = compute_composite_score(transactions)
        label = result.filter(F.col("transaction_id") == "TXN-LOW").select("risk_label").first()[0]
        assert label == "LOW"

    def test_score_between_0_and_1(self, spark, transactions):
        result = compute_composite_score(transactions)
        for row in result.select("risk_score").collect():
            assert 0.0 <= row.risk_score <= 1.0, f"Score {row.risk_score} out of range"


class TestRouteSarQueue:
    def test_sar_eligible_routed_correctly(self, spark, transactions):
        scored = compute_composite_score(transactions)
        sar_df, standard_df = route_sar_queue(scored)
        sar_ids = {r.transaction_id for r in sar_df.select("transaction_id").collect()}
        assert "TXN-SAR" in sar_ids

    def test_low_risk_not_in_sar_queue(self, spark, transactions):
        scored = compute_composite_score(transactions)
        sar_df, _ = route_sar_queue(scored)
        sar_ids = {r.transaction_id for r in sar_df.select("transaction_id").collect()}
        assert "TXN-LOW" not in sar_ids

    def test_sar_has_triggered_at_column(self, spark, transactions):
        scored = compute_composite_score(transactions)
        sar_df, _ = route_sar_queue(scored)
        assert "sar_triggered_at" in sar_df.columns
