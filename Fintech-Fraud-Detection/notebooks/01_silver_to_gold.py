"""
notebooks/01_silver_to_gold.py
Aggregates silver fraud candidates into gold KPI tables and SAR queue.
Includes Delta OPTIMIZE + VACUUM for small file management.

Author     : Narendra Kalisetti
Compliance : MLR 2017 Reg. 35 (SAR), UK GDPR Art. 30
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)


def build_fraud_kpis(df: DataFrame) -> DataFrame:
    """
    Hourly fraud KPIs by transaction type and risk label.
    Used for Power BI live dashboard.
    """
    return (
        df
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("event_timestamp")))
        .groupBy("hour_bucket", "transaction_type", "risk_label")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount_gbp").alias("total_amount_gbp"),
            F.avg("amount_gbp").alias("avg_amount_gbp"),
            F.max("amount_gbp").alias("max_amount_gbp"),
            F.avg("risk_score").alias("avg_risk_score"),
            F.count_if(F.col("risk_label") == "SAR_ELIGIBLE").alias("sar_count"),
        )
        .withColumn("calculated_at", F.current_timestamp())
    )


def build_sar_queue(df: DataFrame) -> DataFrame:
    """
    Extract SAR-eligible transactions for NCA submission pipeline.
    MLR 2017 Reg. 35: sub-30-second detection; SAR within 1 hour.
    """
    return (
        df
        .filter(F.col("risk_label") == "SAR_ELIGIBLE")
        .select(
            "transaction_id",
            "iban_hash",
            "counterparty_iban_hash",
            "amount_gbp",
            "transaction_type",
            "merchant_category",
            "country_code",
            "event_timestamp",
            "risk_score",
            "risk_label",
            F.current_timestamp().alias("sar_triggered_at"),
            F.lit("PENDING_NCA_REVIEW").alias("sar_status"),
        )
    )


def write_gold_merge(spark: SparkSession, df: DataFrame, path: str, merge_keys: list) -> None:
    """Idempotent MERGE into gold Delta table + OPTIMIZE + VACUUM."""
    if DeltaTable.isDeltaTable(spark, path):
        dt = DeltaTable.forPath(spark, path)
        condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        dt.alias("t").merge(df.alias("s"), condition) \
          .whenMatchedUpdateAll() \
          .whenNotMatchedInsertAll() \
          .execute()
    else:
        df.write.format("delta").mode("overwrite").save(path)

    # Delta maintenance — critical for streaming workloads
    # OPTIMIZE compacts small files created by micro-batches
    # ZORDER improves Power BI DirectQuery scan performance
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (hour_bucket)")
    spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")  # 7 days minimum


def run_silver_to_gold(spark: SparkSession, silver_path: str, gold_base: str) -> None:
    logger.info("Starting silver→gold aggregation")

    df_silver = spark.read.format("delta").load(silver_path)

    # Fraud KPIs
    df_kpis = build_fraud_kpis(df_silver)
    write_gold_merge(spark, df_kpis, f"{gold_base}/fraud_kpis/", ["hour_bucket", "transaction_type", "risk_label"])
    logger.info("Fraud KPIs written")

    # SAR queue
    df_sar = build_sar_queue(df_silver)
    write_gold_merge(spark, df_sar, f"{gold_base}/sar_queue/", ["transaction_id"])
    logger.info(f"SAR queue written: {df_sar.count()} SAR-eligible transactions")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("FintechFraud-SilverToGold").getOrCreate()

    SILVER_PATH = "abfss://silver@sauksfraudprod.dfs.core.windows.net/fraud_candidates/"
    GOLD_BASE   = "abfss://gold@sauksfraudprod.dfs.core.windows.net"

    run_silver_to_gold(spark, SILVER_PATH, GOLD_BASE)
