"""
src/risk_scoring.py
Rule-based fraud risk scoring engine.
Applies 5 risk factors to each transaction and produces a composite risk score.

Author     : Narendra Kalisetti
Compliance : MLR 2017 Reg. 21 (Risk Assessment)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import json
import os


# ---------------------------------------------------------------------------
# Risk factor weights (configurable via configs/risk_rules.json)
# ---------------------------------------------------------------------------
DEFAULT_WEIGHTS = {
    "high_value_weight":       0.35,   # MLR 2017 Reg. 27: >= £5,000
    "international_weight":    0.20,   # Cross-border flag
    "high_risk_category_weight": 0.20, # Merchant category risk
    "velocity_weight":         0.15,   # Transaction velocity
    "geo_mismatch_weight":     0.10,   # IP country vs card country mismatch
}

# UK National Crime Agency high-risk merchant categories
HIGH_RISK_MERCHANT_CATEGORIES = [
    "CRYPTO", "GAMBLING", "MONEY_TRANSFER", "WIRE_TRANSFER",
    "PREPAID_CARDS", "FOREIGN_EXCHANGE", "JEWELLERY",
]

# High-risk countries (FATF grey/black list as of 2024)
HIGH_RISK_COUNTRIES = [
    "KP", "IR", "MM", "BY", "RU", "SY", "CU", "VE",
    "YE", "LY", "SO", "SD",
]


def load_risk_rules(config_path: str = None) -> dict:
    """Load risk weights from JSON config. Falls back to defaults."""
    if config_path and os.path.exists(config_path):
        with open(config_path) as f:
            return json.load(f)
    return DEFAULT_WEIGHTS


def score_high_value(df: DataFrame, threshold: float = 5000.0) -> DataFrame:
    """Risk factor 1: High-value transaction (MLR 2017 Reg. 27)."""
    return df.withColumn(
        "risk_high_value",
        F.when(F.col("amount_gbp") >= threshold, 1.0).otherwise(0.0)
    )


def score_international(df: DataFrame) -> DataFrame:
    """Risk factor 2: International transaction flag."""
    return df.withColumn(
        "risk_international",
        F.when(
            F.col("is_international") | F.col("country_code").isin(HIGH_RISK_COUNTRIES),
            1.0
        ).otherwise(0.0)
    )


def score_merchant_category(df: DataFrame) -> DataFrame:
    """Risk factor 3: High-risk merchant category."""
    return df.withColumn(
        "risk_merchant_category",
        F.when(
            F.upper(F.col("merchant_category")).isin(HIGH_RISK_MERCHANT_CATEGORIES),
            1.0
        ).otherwise(0.0)
    )


def score_geo_mismatch(df: DataFrame) -> DataFrame:
    """
    Risk factor 4: Geography mismatch between IP country and transaction country.
    A UK card transacting from a non-UK IP is elevated risk.
    """
    return df.withColumn(
        "risk_geo_mismatch",
        F.when(
            F.col("ip_country").isNotNull() &
            F.col("country_code").isNotNull() &
            (F.col("ip_country") != F.col("country_code")),
            1.0
        ).otherwise(0.0)
    )


def score_large_round_amount(df: DataFrame) -> DataFrame:
    """
    Risk factor 5: Large round-number amounts are a money laundering signal.
    E.g. £10,000.00, £50,000.00 exactly.
    """
    return df.withColumn(
        "risk_round_amount",
        F.when(
            (F.col("amount_gbp") >= 1000) &
            ((F.col("amount_gbp") % 1000) == 0),
            1.0
        ).otherwise(0.0)
    )


def compute_composite_score(df: DataFrame, weights: dict = None) -> DataFrame:
    """
    Compute weighted composite risk score (0.0 – 1.0).
    Score >= 0.6 → SAR-eligible (MLR 2017 Reg. 35).
    Score >= 0.4 → Enhanced Due Diligence (MLR 2017 Reg. 27).
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS

    w = weights
    scored = (
        df
        .transform(score_high_value)
        .transform(score_international)
        .transform(score_merchant_category)
        .transform(score_geo_mismatch)
        .transform(score_large_round_amount)
    )

    scored = scored.withColumn(
        "risk_score",
        F.round(
            (F.col("risk_high_value")         * w.get("high_value_weight", 0.35)) +
            (F.col("risk_international")       * w.get("international_weight", 0.20)) +
            (F.col("risk_merchant_category")   * w.get("high_risk_category_weight", 0.20)) +
            (F.col("risk_geo_mismatch")        * w.get("geo_mismatch_weight", 0.10)) +
            (F.col("risk_round_amount")        * w.get("velocity_weight", 0.15)),
            4
        ).cast(DoubleType())
    )

    return scored.withColumn(
        "risk_label",
        F.when(F.col("risk_score") >= 0.6, "SAR_ELIGIBLE")
         .when(F.col("risk_score") >= 0.4, "ENHANCED_DUE_DILIGENCE")
         .when(F.col("risk_score") >= 0.2, "ELEVATED")
         .otherwise("LOW")
    )


def route_sar_queue(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Route SAR-eligible events to SAR queue; rest to standard fraud candidates.
    MLR 2017 Reg. 35: SAR submission to NCA required for confirmed suspicious activity.
    """
    sar_df = df.filter(F.col("risk_label") == "SAR_ELIGIBLE") \
               .withColumn("sar_triggered_at", F.current_timestamp())

    standard_df = df.filter(F.col("risk_label") != "SAR_ELIGIBLE")

    return sar_df, standard_df


def apply_risk_scoring(df: DataFrame, config_path: str = None) -> DataFrame:
    """Full pipeline: apply all risk factors and compute composite score."""
    weights = load_risk_rules(config_path)
    return compute_composite_score(df, weights)
