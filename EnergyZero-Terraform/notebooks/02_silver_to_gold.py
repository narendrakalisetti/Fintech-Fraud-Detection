"""
Notebook: 02_silver_to_gold.py
Purpose : Aggregate silver meter readings into Net Zero 2050 KPIs,
          grid stability metrics, and executive dashboard tables.
Author  : Narendra Kalisetti
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)


def build_net_zero_summary(df: DataFrame) -> DataFrame:
    """
    Regional renewable output vs. carbon targets per day.
    KPI: renewable_kwh / total_kwh = renewable_share
    UK Net Zero 2050 target: 100% renewable share.
    """
    # Ofgem tariff codes: "GREEN", "RENEWABLE", "EV" are clean energy
    renewable_tariffs = ["GREEN", "RENEWABLE", "EV", "SOLAR"]

    return (
        df
        .withColumn(
            "is_renewable",
            F.col("tariff_type").isin(renewable_tariffs).cast("int")
        )
        .groupBy(
            F.col("ingest_date").alias("report_date"),
            F.col("region_code")
        )
        .agg(
            F.sum("kwh_consumed").alias("total_kwh"),
            F.sum(F.col("kwh_consumed") * F.col("is_renewable")).alias("renewable_kwh"),
            F.avg("kwh_consumed").alias("avg_consumption_kwh"),
            F.count("reading_id").alias("reading_count"),
        )
        .withColumn(
            "renewable_share_pct",
            F.round(F.col("renewable_kwh") / F.col("total_kwh") * 100, 2)
        )
        .withColumn(
            "net_zero_gap_pct",
            F.round(100 - F.col("renewable_share_pct"), 2)
        )
        .withColumn("calculated_at", F.current_timestamp())
    )


def build_grid_kpis(df: DataFrame) -> DataFrame:
    """
    Daily grid voltage stability KPIs per region.
    UK grid nominal voltage: 230V (±10% tolerance = 207–253V).
    """
    return (
        df
        .filter(F.col("voltage").isNotNull())
        .groupBy(
            F.col("ingest_date").alias("report_date"),
            F.col("region_code")
        )
        .agg(
            F.avg("voltage").alias("avg_voltage"),
            F.min("voltage").alias("min_voltage"),
            F.max("voltage").alias("max_voltage"),
            F.stddev("voltage").alias("voltage_stddev"),
            F.count_if(F.col("voltage") < 207).alias("under_voltage_events"),
            F.count_if(F.col("voltage") > 253).alias("over_voltage_events"),
            F.count("reading_id").alias("total_readings"),
        )
        .withColumn(
            "stability_score",
            F.round(
                1 - (
                    (F.col("under_voltage_events") + F.col("over_voltage_events"))
                    / F.col("total_readings")
                ), 4
            )
        )
        .withColumn("calculated_at", F.current_timestamp())
    )


def write_gold_delta(spark: SparkSession, df: DataFrame, gold_path: str, merge_keys: list) -> None:
    """
    Merge into gold Delta table. OPTIMIZE + ZORDER for Power BI DirectQuery.
    """
    if DeltaTable.isDeltaTable(spark, gold_path):
        delta_table = DeltaTable.forPath(spark, gold_path)
        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(gold_path)

    spark.sql(f"OPTIMIZE delta.`{gold_path}`")
    spark.sql(f"VACUUM delta.`{gold_path}` RETAIN 168 HOURS")


def run_silver_to_gold(spark: SparkSession, silver_path: str, gold_base: str) -> None:
    logger.info("Starting silver→gold aggregation")

    df_silver = spark.read.format("delta").load(silver_path)

    # Net Zero Summary
    df_net_zero = build_net_zero_summary(df_silver)
    write_gold_delta(
        spark, df_net_zero,
        f"{gold_base}/net_zero_summary/",
        merge_keys=["report_date", "region_code"]
    )
    logger.info("Net Zero summary written to gold")

    # Grid KPIs
    df_grid = build_grid_kpis(df_silver)
    write_gold_delta(
        spark, df_grid,
        f"{gold_base}/grid_kpis/",
        merge_keys=["report_date", "region_code"]
    )
    logger.info("Grid KPIs written to gold")

    # Executive report (pre-aggregated for Power BI DirectQuery)
    df_exec = (
        df_net_zero
        .join(df_grid, on=["report_date", "region_code"], how="left")
        .orderBy("report_date", "region_code")
    )
    write_gold_delta(
        spark, df_exec,
        f"{gold_base}/executive_report/",
        merge_keys=["report_date", "region_code"]
    )
    logger.info("Executive report written to gold")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("EnergyZero-SilverToGold").getOrCreate()

    SILVER_PATH = "abfss://silver@sauksenergyzeroprod.dfs.core.windows.net/meter_readings/"
    GOLD_BASE   = "abfss://gold@sauksenergyzeroprod.dfs.core.windows.net"

    run_silver_to_gold(spark, SILVER_PATH, GOLD_BASE)
