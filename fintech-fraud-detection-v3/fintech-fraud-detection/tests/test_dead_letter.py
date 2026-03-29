"""Unit tests for the Dead-Letter Handler."""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, LongType
)
from src.dead_letter_handler import capture_dead_letter_events


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("DeadLetter-Tests")
        .getOrCreate()
    )


SCHEMA = StructType([
    StructField("Transaction_ID",         StringType(),    True),
    StructField("Event_Timestamp",        TimestampType(), True),
    StructField("Customer_Account_IBAN",  StringType(),    True),
    StructField("Transaction_Amount_GBP", DoubleType(),    True),
    StructField("body_str",              StringType(),    True),
])

ROWS = [
    ("TXN001", datetime(2024,6,1), "GB29NWBK001", 6000.0, '{"Transaction_ID":"TXN001"}'),
    (None,     datetime(2024,6,1), "GB29NWBK002", 7000.0, '{"Transaction_Amount_GBP":7000}'),
    ("TXN003", None,               "GB29NWBK003", 8000.0, '{"Transaction_ID":"TXN003"}'),
    ("TXN004", datetime(2024,6,1), "GB29NWBK004", None,   '{"Transaction_ID":"TXN004"}'),
]


class TestDeadLetterHandler:
    def get_df(self, spark):
        return spark.createDataFrame(ROWS, schema=SCHEMA)

    def test_valid_events_not_in_dead_letter(self, spark):
        df = self.get_df(spark)
        dead = capture_dead_letter_events(df)
        ids = [r.Transaction_ID for r in dead.select("Transaction_ID").collect()]
        assert "TXN001" not in ids

    def test_null_transaction_id_captured(self, spark):
        df = self.get_df(spark)
        dead = capture_dead_letter_events(df)
        reasons = [r.failure_reason for r in dead.select("failure_reason").collect()]
        assert "NULL_TRANSACTION_ID" in reasons

    def test_null_timestamp_captured(self, spark):
        df = self.get_df(spark)
        dead = capture_dead_letter_events(df)
        reasons = [r.failure_reason for r in dead.select("failure_reason").collect()]
        assert "NULL_EVENT_TIMESTAMP" in reasons

    def test_null_amount_captured(self, spark):
        df = self.get_df(spark)
        dead = capture_dead_letter_events(df)
        reasons = [r.failure_reason for r in dead.select("failure_reason").collect()]
        assert "NULL_AMOUNT" in reasons

    def test_dead_letter_count(self, spark):
        df = self.get_df(spark)
        dead = capture_dead_letter_events(df)
        assert dead.count() == 3   # 3 of 4 rows are malformed

    def test_review_status_pending(self, spark):
        df = self.get_df(spark)
        dead = capture_dead_letter_events(df)
        statuses = [r.review_status for r in dead.select("review_status").collect()]
        assert all(s == "PENDING" for s in statuses)

    def test_metadata_columns_present(self, spark):
        df = self.get_df(spark)
        dead = capture_dead_letter_events(df)
        for col in ["failure_reason", "dead_letter_timestamp",
                    "pipeline_name", "review_status", "compliance_priority"]:
            assert col in dead.columns
