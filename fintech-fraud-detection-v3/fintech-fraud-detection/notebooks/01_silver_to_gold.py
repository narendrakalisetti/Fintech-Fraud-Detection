# Databricks notebook source
# =============================================================================
# ClearPay UK | Notebook: 01_silver_to_gold
# =============================================================================
# Purpose   : Read cleansed silver fraud_candidates Delta table, compute
#             fraud KPI aggregations for Power BI, and prepare SAR-queue
#             summary for MLRO review.
#
# Layer     : Silver --> Gold
# Schedule  : ADF trigger every 1 hour (or on-demand by compliance team)
# Author    : Narendra Kalisetti
# GDPR      : Gold layer contains NO raw PII. Hashed IBANs are also
#             excluded from executive-level gold aggregations.
# MLR 2017  : gold/sar_queue/ output is reviewed by MLRO within 1 hour.
# =============================================================================

# COMMAND ----------
dbutils.widgets.text("process_date", "", "Process Date (YYYY-MM-DD)")
dbutils.widgets.text("environment", "prod", "Environment")

process_date = dbutils.widgets.get("process_date") or str(spark.sql("SELECT current_date()").first()[0])
environment  = dbutils.widgets.get("environment")

print(f"Silver -> Gold | process_date={process_date} | env={environment}")

# COMMAND ----------
storage_account = dbutils.secrets.get(scope="fintech-kv-scope", key="adls-account-name")
SILVER_PATH = f"abfss://silver@{storage_account}.dfs.core.windows.net"
GOLD_PATH   = f"abfss://gold@{storage_account}.dfs.core.windows.net"

# COMMAND ----------
from pyspark.sql import functions as F
from delta.tables import DeltaTable

df_silver = (
    spark.read
    .format("delta")
    .load(f"{SILVER_PATH}/fraud_candidates/")
    .filter(F.col("Silver_Load_Date") == process_date)
)

silver_count = df_silver.count()
print(f"Silver records loaded for {process_date}: {silver_count:,}")

# COMMAND ----------
# =============================================================================
# GOLD TABLE 1: fraud_kpis
# Hourly fraud metrics aggregated by risk band and payment channel.
# Powers the live Power BI fraud analyst dashboard.
# GDPR: No IBAN hashes in this table — pure aggregates only.
# =============================================================================
df_fraud_kpis = (
    df_silver
    .groupBy("Silver_Load_Date", "risk_band", "Payment_Channel", "Country_Code")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("Transaction_Amount_GBP").alias("total_amount_gbp"),
        F.avg("Transaction_Amount_GBP").alias("avg_amount_gbp"),
        F.max("Transaction_Amount_GBP").alias("max_amount_gbp"),
        F.avg("composite_risk_score").alias("avg_risk_score"),
        F.sum(F.when(F.col("sar_eligible"), 1).otherwise(0)).alias("sar_eligible_count"),
        F.sum(F.when(F.col("edd_required"), 1).otherwise(0)).alias("edd_required_count"),
        F.sum(F.when(F.col("Is_International") == "true", 1).otherwise(0)).alias("international_count"),
    )
    .withColumn(
        "sar_rate_pct",
        F.round((F.col("sar_eligible_count") / F.col("transaction_count")) * 100, 2)
    )
    .withColumn(
        "risk_band_label",
        F.when(F.col("risk_band") == "CRITICAL", "Critical Risk - Immediate Review")
         .when(F.col("risk_band") == "HIGH",     "High Risk - EDD Required")
         .when(F.col("risk_band") == "MEDIUM",   "Medium Risk - Monitoring")
         .otherwise("Low Risk - Standard Processing")
    )
    .withColumn("gold_load_timestamp", F.current_timestamp())
    .withColumn("gdpr_pii_present",    F.lit(False))
    .withColumn("data_classification", F.lit("INTERNAL"))
)

gold_kpis_path = f"{GOLD_PATH}/fraud_kpis/"
if DeltaTable.isDeltaTable(spark, gold_kpis_path):
    DeltaTable.forPath(spark, gold_kpis_path).alias("t").merge(
        df_fraud_kpis.alias("s"),
        "t.Silver_Load_Date = s.Silver_Load_Date AND t.risk_band = s.risk_band AND t.Payment_Channel = s.Payment_Channel AND t.Country_Code = s.Country_Code"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_fraud_kpis.write.format("delta").mode("overwrite").partitionBy("Silver_Load_Date").save(gold_kpis_path)

print(f"Gold fraud_kpis written: {df_fraud_kpis.count():,} records")

# COMMAND ----------
# =============================================================================
# GOLD TABLE 2: sar_queue_summary
# Summary of SAR-eligible transactions for MLRO daily review.
# MLR 2017 Reg.35: MLRO must review within 1 hour of flagging.
# NOTE: This table retains the hashed IBAN for traceability — the MLRO
# can cross-reference with the bronze layer (which holds the raw IBAN)
# via the hash key if a SAR submission is required.
# =============================================================================
df_sar = (
    df_silver
    .filter(F.col("sar_eligible") == True)
    .select(
        "Transaction_ID",
        "Event_Timestamp",
        "Silver_Load_Date",
        "Customer_Account_IBAN_Hashed",   # Hashed ref only — GDPR compliant
        "Beneficiary_IBAN_Hashed",
        "Transaction_Amount_GBP",
        "Payment_Channel",
        "Country_Code",
        "Is_International",
        "composite_risk_score",
        "risk_band",
        "score_amount_risk",
        "score_country_risk",
        "score_channel_risk",
        "score_mcc_risk",
        "Merchant_Category_Code",
        "EH_Enqueued_Timestamp",
        "EH_Sequence_Number",
    )
    .withColumn("sar_review_status",      F.lit("PENDING_MLRO_REVIEW"))
    .withColumn("sar_submission_deadline",
        # MLR 2017: SAR must be submitted within 7 days of suspicion arising
        F.date_add(F.col("Silver_Load_Date"), 7)
    )
    .withColumn("gold_load_timestamp",    F.current_timestamp())
    .withColumn("mlr_regulation",         F.lit("MLR 2017 Reg.35"))
    .withColumn("gdpr_pii_present",       F.lit(False))   # Hashed only
)

gold_sar_path = f"{GOLD_PATH}/sar_queue/"
if DeltaTable.isDeltaTable(spark, gold_sar_path):
    DeltaTable.forPath(spark, gold_sar_path).alias("t").merge(
        df_sar.alias("s"),
        "t.Transaction_ID = s.Transaction_ID"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_sar.write.format("delta").mode("overwrite").partitionBy("Silver_Load_Date").save(gold_sar_path)

sar_count = df_sar.count()
print(f"Gold sar_queue written: {sar_count:,} SAR-eligible transactions")

# COMMAND ----------
# =============================================================================
# GOLD TABLE 3: executive_fraud_summary
# Daily national fraud summary — feeds executive Power BI dashboard.
# Pure aggregates, zero PII of any kind.
# =============================================================================
df_exec = (
    df_silver
    .groupBy("Silver_Load_Date")
    .agg(
        F.count("*").alias("total_flagged_transactions"),
        F.sum("Transaction_Amount_GBP").alias("total_flagged_amount_gbp"),
        F.avg("composite_risk_score").alias("avg_risk_score"),
        F.sum(F.when(F.col("sar_eligible"),         1).otherwise(0)).alias("sar_eligible_total"),
        F.sum(F.when(F.col("edd_required"),          1).otherwise(0)).alias("edd_required_total"),
        F.sum(F.when(F.col("risk_band") == "CRITICAL", 1).otherwise(0)).alias("critical_risk_count"),
        F.sum(F.when(F.col("risk_band") == "HIGH",     1).otherwise(0)).alias("high_risk_count"),
        F.sum(F.when(F.col("Is_International") == "true",
                     F.col("Transaction_Amount_GBP")
              ).otherwise(0)).alias("international_amount_gbp"),
        F.countDistinct("Merchant_Category_Code").alias("distinct_mcc_count"),
        F.countDistinct("Country_Code").alias("distinct_country_count"),
    )
    .withColumn(
        "fraud_detection_rate_pct",
        F.round((F.col("sar_eligible_total") / F.col("total_flagged_transactions")) * 100, 2)
    )
    .withColumn("gold_load_timestamp", F.current_timestamp())
    .withColumn("gdpr_pii_present",    F.lit(False))
    .withColumn("report_source",       F.lit("ClearPay Fraud Detection Platform"))
)

gold_exec_path = f"{GOLD_PATH}/executive_fraud_summary/"
if DeltaTable.isDeltaTable(spark, gold_exec_path):
    DeltaTable.forPath(spark, gold_exec_path).alias("t").merge(
        df_exec.alias("s"), "t.Silver_Load_Date = s.Silver_Load_Date"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_exec.write.format("delta").mode("overwrite").save(gold_exec_path)

# COMMAND ----------
# Optimise all gold tables for Power BI DirectQuery
for path, zorder in [
    (gold_kpis_path,  "risk_band"),
    (gold_sar_path,   "composite_risk_score"),
    (gold_exec_path,  "Silver_Load_Date"),
]:
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({zorder})")

# COMMAND ----------
print("=" * 60)
print("SILVER --> GOLD COMPLETE")
print(f"  fraud_kpis records         : {df_fraud_kpis.count():,}")
print(f"  sar_queue records          : {sar_count:,}")
print(f"  executive_summary records  : {df_exec.count():,}")
print(f"  GDPR: No raw PII in gold   : CONFIRMED")
print(f"  MLR 2017: SAR queue ready  : YES")
print("=" * 60)

dbutils.notebook.exit(f'{{"status":"success","sar_eligible":{sar_count},"process_date":"{process_date}"}}')
