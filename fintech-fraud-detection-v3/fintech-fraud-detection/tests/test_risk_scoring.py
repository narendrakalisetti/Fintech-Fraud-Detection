"""
Unit tests for the Risk Scoring Engine.
Tests all scoring dimensions and composite score calculation.
"""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from src.risk_scoring import apply_risk_scoring, HIGH_VALUE_THRESHOLD_GBP


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("ClearPay-RiskScoring-Tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


SCHEMA = StructType([
    StructField("Transaction_ID",           StringType(),    False),
    StructField("Event_Timestamp",          TimestampType(), False),
    StructField("Customer_Account_IBAN_Hashed", StringType(), False),
    StructField("Transaction_Amount_GBP",   DoubleType(),    False),
    StructField("Payment_Channel",          StringType(),    True),
    StructField("Country_Code",             StringType(),    True),
    StructField("Is_International",         StringType(),    True),
    StructField("Merchant_Category_Code",   StringType(),    True),
])


def make_txn(spark, txn_id="T001", amount=6000.0, channel="CARD",
             country="GBR", international="false", mcc="5411"):
    row = [(txn_id, datetime(2024, 6, 1, 10, 0, 0),
            "abc123hash", amount, channel, country, international, mcc)]
    return spark.createDataFrame(row, schema=SCHEMA)


class TestAmountRiskScore:
    def test_just_above_threshold_low_score(self, spark):
        df = make_txn(spark, amount=5001.0)
        result = apply_risk_scoring(df).first()
        assert 0.0 <= result.score_amount_risk <= 0.05

    def test_very_high_amount_high_score(self, spark):
        df = make_txn(spark, amount=95000.0)
        result = apply_risk_scoring(df).first()
        assert result.score_amount_risk >= 0.9

    def test_amount_capped_at_1(self, spark):
        df = make_txn(spark, amount=200000.0)
        result = apply_risk_scoring(df).first()
        assert result.score_amount_risk == 1.0


class TestCountryRiskScore:
    def test_domestic_uk_low_risk(self, spark):
        df = make_txn(spark, country="GBR")
        result = apply_risk_scoring(df).first()
        assert result.score_country_risk == 0.1

    def test_fatf_high_risk_country(self, spark):
        df = make_txn(spark, country="IRN")  # Iran — FATF blacklist
        result = apply_risk_scoring(df).first()
        assert result.score_country_risk == 1.0

    def test_another_high_risk_country(self, spark):
        df = make_txn(spark, country="PRK")  # North Korea
        result = apply_risk_scoring(df).first()
        assert result.score_country_risk == 1.0


class TestChannelRiskScore:
    def test_swift_highest_risk(self, spark):
        df = make_txn(spark, channel="SWIFT")
        result = apply_risk_scoring(df).first()
        assert result.score_channel_risk == 0.9

    def test_card_lowest_risk(self, spark):
        df = make_txn(spark, channel="CARD")
        result = apply_risk_scoring(df).first()
        assert result.score_channel_risk == 0.2

    def test_chaps_medium_risk(self, spark):
        df = make_txn(spark, channel="CHAPS")
        result = apply_risk_scoring(df).first()
        assert result.score_channel_risk == 0.6

    def test_unknown_channel_defaults_to_medium(self, spark):
        df = make_txn(spark, channel="UNKNOWN_CHANNEL")
        result = apply_risk_scoring(df).first()
        assert result.score_channel_risk == 0.5


class TestMCCRiskScore:
    def test_high_risk_mcc_atm(self, spark):
        df = make_txn(spark, mcc="6011")   # ATM cash withdrawal
        result = apply_risk_scoring(df).first()
        assert result.score_mcc_risk == 0.9

    def test_high_risk_mcc_gambling(self, spark):
        df = make_txn(spark, mcc="7995")   # Gambling
        result = apply_risk_scoring(df).first()
        assert result.score_mcc_risk == 0.9

    def test_normal_mcc_low_risk(self, spark):
        df = make_txn(spark, mcc="5411")   # Grocery stores
        result = apply_risk_scoring(df).first()
        assert result.score_mcc_risk == 0.1


class TestInternationalRiskScore:
    def test_international_high_risk(self, spark):
        df = make_txn(spark, international="true")
        result = apply_risk_scoring(df).first()
        assert result.score_international_risk == 0.8

    def test_domestic_low_risk(self, spark):
        df = make_txn(spark, international="false")
        result = apply_risk_scoring(df).first()
        assert result.score_international_risk == 0.1


class TestCompositeScore:
    def test_score_between_0_and_1(self, spark):
        df = make_txn(spark)
        result = apply_risk_scoring(df).first()
        assert 0.0 <= result.composite_risk_score <= 1.0

    def test_high_risk_transaction_high_score(self, spark):
        """SWIFT, Iran, ATM, international, £80k — should be CRITICAL"""
        df = make_txn(spark, amount=80000.0, channel="SWIFT",
                      country="IRN", international="true", mcc="6011")
        result = apply_risk_scoring(df).first()
        assert result.composite_risk_score >= 0.75
        assert result.risk_band == "CRITICAL"
        assert result.sar_eligible is True
        assert result.edd_required is True

    def test_low_risk_transaction_low_score(self, spark):
        """Card, UK, grocery, domestic, £5,001 — should be LOW"""
        df = make_txn(spark, amount=5001.0, channel="CARD",
                      country="GBR", international="false", mcc="5411")
        result = apply_risk_scoring(df).first()
        assert result.risk_band == "LOW"
        assert result.sar_eligible is False

    def test_risk_bands_correct(self, spark):
        rows = [
            ("T1", datetime(2024, 1, 1), "h1", 80000.0, "SWIFT", "IRN", "true",  "6011"),
            ("T2", datetime(2024, 1, 1), "h2", 30000.0, "CHAPS", "PAK", "true",  "6010"),
            ("T3", datetime(2024, 1, 1), "h3", 6000.0,  "FPS",   "GBR", "false", "5411"),
            ("T4", datetime(2024, 1, 1), "h4", 5001.0,  "CARD",  "GBR", "false", "5411"),
        ]
        df = spark.createDataFrame(rows, schema=SCHEMA)
        results = {r.Transaction_ID: r.risk_band
                   for r in apply_risk_scoring(df).select("Transaction_ID", "risk_band").collect()}
        assert results["T1"] in ("CRITICAL", "HIGH")
        assert results["T4"] in ("LOW", "MEDIUM")

    def test_sar_eligible_at_075_threshold(self, spark):
        """Transactions just below 0.75 should not be SAR eligible"""
        df = make_txn(spark, amount=5001.0, channel="CARD",
                      country="GBR", international="false", mcc="5411")
        result = apply_risk_scoring(df).first()
        assert result.sar_eligible is False
