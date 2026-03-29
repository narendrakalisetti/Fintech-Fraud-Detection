"""
Integration Tests — ClearPay Fraud Detection Pipeline
Tests the full pipeline transformation chain end-to-end using
local PySpark (no Event Hubs or ADLS required).
"""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, LongType
)

from src.fraud_detection_stream import (
    filter_high_value_transactions,
    hash_pii_columns,
    add_pipeline_metadata,
    HIGH_VALUE_THRESHOLD_GBP,
)
from src.risk_scoring import apply_risk_scoring
from src.dead_letter_handler import capture_dead_letter_events


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("ClearPay-Integration-Tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


FULL_SCHEMA = StructType([
    StructField("Transaction_ID",           StringType(),    True),
    StructField("Event_Timestamp",          TimestampType(), True),
    StructField("Customer_Account_IBAN",    StringType(),    True),
    StructField("Beneficiary_IBAN",         StringType(),    True),
    StructField("Merchant_ID",              StringType(),    True),
    StructField("Merchant_Category_Code",   StringType(),    True),
    StructField("Transaction_Amount_GBP",   DoubleType(),    True),
    StructField("Transaction_Currency",     StringType(),    True),
    StructField("Payment_Channel",          StringType(),    True),
    StructField("Country_Code",             StringType(),    True),
    StructField("Is_International",         StringType(),    True),
    StructField("Risk_Score",               DoubleType(),    True),
    StructField("Ingestion_Timestamp",      LongType(),      True),
    StructField("body_str",                 StringType(),    True),
    StructField("EH_Enqueued_Timestamp",    TimestampType(), True),
    StructField("EH_Offset",               StringType(),    True),
    StructField("EH_Sequence_Number",       LongType(),      True),
])

SAMPLE_ROWS = [
    # High-value, high-risk (SWIFT, Iran, ATM) — should reach SAR queue
    ("TXN001", datetime(2024,6,1,10,0,0), "GB29NWBK60161331926819", "IR29NWBK60161331926820",
     "M001", "6011", 85000.0, "GBP", "SWIFT", "IRN", "true",  0.2, 1717228800000,
     '{"Transaction_ID":"TXN001"}', datetime(2024,6,1,10,0,1), "100", 1),
    # High-value, medium risk (CHAPS, UK) — silver only
    ("TXN002", datetime(2024,6,1,10,1,0), "GB29NWBK60161331926820", None,
     "M002", "5411", 12000.0, "GBP", "CHAPS", "GBR", "false", 0.3, 1717228860000,
     '{"Transaction_ID":"TXN002"}', datetime(2024,6,1,10,1,1), "101", 2),
    # Below threshold — should be filtered out of silver
    ("TXN003", datetime(2024,6,1,10,2,0), "GB29NWBK60161331926821", None,
     "M003", "5411", 4999.99, "GBP", "CARD",  "GBR", "false", 0.1, 1717228920000,
     '{"Transaction_ID":"TXN003"}', datetime(2024,6,1,10,2,1), "102", 3),
    # Null Transaction_ID — should go to dead letter
    (None, datetime(2024,6,1,10,3,0), "GB29NWBK60161331926822", None,
     "M004", "7995", 7500.0, "GBP", "FPS",   "GBR", "false", 0.5, 1717228980000,
     '{"Transaction_Amount_GBP":7500}', datetime(2024,6,1,10,3,1), "103", 4),
    # High-value, low risk (Card, UK, grocery)
    ("TXN005", datetime(2024,6,1,10,4,0), "GB29NWBK60161331926823", None,
     "M005", "5411", 6500.0, "GBP", "CARD",  "GBR", "false", 0.15, 1717229040000,
     '{"Transaction_ID":"TXN005"}', datetime(2024,6,1,10,4,1), "104", 5),
]

TEST_SALT = "test_salt_value_not_for_production"


class TestFullPipelineChain:
    def get_df(self, spark):
        return spark.createDataFrame(SAMPLE_ROWS, schema=FULL_SCHEMA)

    def test_filter_removes_below_threshold(self, spark):
        df = self.get_df(spark)
        filtered = filter_high_value_transactions(df)
        ids = [r.Transaction_ID for r in filtered.select("Transaction_ID").collect()]
        assert "TXN003" not in ids       # £4,999.99 — below threshold
        assert "TXN001" in ids           # £85,000 — above
        assert "TXN002" in ids           # £12,000 — above

    def test_filter_removes_null_mandatory_fields(self, spark):
        df = self.get_df(spark)
        filtered = filter_high_value_transactions(df)
        ids = [r.Transaction_ID for r in filtered.select("Transaction_ID").collect()]
        assert None not in ids           # Null Transaction_ID filtered out

    def test_gdpr_hash_removes_raw_iban(self, spark):
        df = self.get_df(spark)
        filtered = filter_high_value_transactions(df)
        hashed = hash_pii_columns(filtered, TEST_SALT)
        assert "Customer_Account_IBAN" not in hashed.columns
        assert "Beneficiary_IBAN" not in hashed.columns
        assert "Customer_Account_IBAN_Hashed" in hashed.columns
        assert "Beneficiary_IBAN_Hashed" in hashed.columns

    def test_hash_is_64_char_sha256(self, spark):
        df = self.get_df(spark)
        filtered = filter_high_value_transactions(df)
        hashed = hash_pii_columns(filtered, TEST_SALT)
        hashes = [r.Customer_Account_IBAN_Hashed
                  for r in hashed.select("Customer_Account_IBAN_Hashed").collect()]
        for h in hashes:
            assert len(h) == 64, f"SHA-256 must produce 64 hex chars, got {len(h)}"

    def test_risk_scoring_adds_required_columns(self, spark):
        df = self.get_df(spark)
        filtered = filter_high_value_transactions(df)
        hashed = hash_pii_columns(filtered, TEST_SALT)
        scored = apply_risk_scoring(hashed)
        for col in ["composite_risk_score", "risk_band", "sar_eligible", "edd_required"]:
            assert col in scored.columns, f"Missing column: {col}"

    def test_high_risk_transaction_sar_eligible(self, spark):
        """TXN001: SWIFT + Iran + ATM + £85k should be SAR eligible"""
        df = self.get_df(spark)
        filtered = filter_high_value_transactions(df)
        hashed = hash_pii_columns(filtered, TEST_SALT)
        scored = apply_risk_scoring(hashed)
        txn001 = scored.filter(F.col("Transaction_ID") == "TXN001").first()
        assert txn001 is not None
        assert txn001.sar_eligible is True
        assert txn001.risk_band in ("CRITICAL", "HIGH")

    def test_metadata_columns_present(self, spark):
        df = self.get_df(spark)
        filtered = filter_high_value_transactions(df)
        hashed = hash_pii_columns(filtered, TEST_SALT)
        scored = apply_risk_scoring(hashed)
        enriched = add_pipeline_metadata(scored)
        for col in ["Pipeline_Name", "GDPR_PII_Hashed", "MLR_2017_Flagged",
                    "Data_Classification", "Silver_Load_Date", "Layer"]:
            assert col in enriched.columns

    def test_gdpr_flag_always_true(self, spark):
        df = self.get_df(spark)
        filtered = filter_high_value_transactions(df)
        hashed = hash_pii_columns(filtered, TEST_SALT)
        scored = apply_risk_scoring(hashed)
        enriched = add_pipeline_metadata(scored)
        flags = [r.GDPR_PII_Hashed
                 for r in enriched.select("GDPR_PII_Hashed").collect()]
        assert all(flags), "GDPR_PII_Hashed must be True on all records"

    def test_dead_letter_captures_null_transaction_id(self, spark):
        df = self.get_df(spark)
        dead = capture_dead_letter_events(df)
        dead_ids = dead.select("failure_reason").collect()
        assert len(dead_ids) >= 1
        reasons = [r.failure_reason for r in dead_ids]
        assert "NULL_TRANSACTION_ID" in reasons

    def test_full_chain_record_counts(self, spark):
        """Verify record flow through the full pipeline"""
        df = self.get_df(spark)
        total_input = df.count()                          # 5 records
        filtered = filter_high_value_transactions(df)
        silver_count = filtered.count()                   # 4 (TXN001,002,005 + no null)
        dead_count = capture_dead_letter_events(df).count()  # 1 (null Transaction_ID)

        assert total_input == 5
        assert silver_count == 3   # TXN001, TXN002, TXN005 (TXN003 below threshold, null filtered)
        assert dead_count == 1     # TXN with null Transaction_ID

    def test_salt_affects_hash_output(self, spark):
        """Different salts produce different hashes — ensuring salt is applied"""
        df = self.get_df(spark)
        filtered = filter_high_value_transactions(df)
        hashed_a = hash_pii_columns(filtered, "salt_A")
        hashed_b = hash_pii_columns(filtered, "salt_B")
        hash_a = hashed_a.filter(F.col("Transaction_ID") == "TXN001") \
                         .first().Customer_Account_IBAN_Hashed
        hash_b = hashed_b.filter(F.col("Transaction_ID") == "TXN001") \
                         .first().Customer_Account_IBAN_Hashed
        assert hash_a != hash_b, "Different salts must produce different hashes"
