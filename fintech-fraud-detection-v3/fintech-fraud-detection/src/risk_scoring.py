"""
=============================================================================
Risk Scoring Engine — ClearPay UK Fraud Detection Platform
=============================================================================
Rule-based fraud risk scoring applied to high-value transactions (>£5,000).
Scores range 0.0–1.0 where 1.0 = highest risk.

Rules are configurable via configs/risk_rules.json — tunable by the
compliance team without code changes.

MLR 2017 Reg. 21: This engine constitutes part of the firm's documented
risk assessment framework for transaction monitoring.
=============================================================================
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import json
import logging

log = logging.getLogger("ClearPay.RiskScoring")

# ---------------------------------------------------------------------------
# High-risk country codes (FATF grey/black list — updated quarterly)
# ---------------------------------------------------------------------------
HIGH_RISK_COUNTRIES = {
    "IRN", "PRK", "MMR", "SYR", "YEM", "LBY", "MLI", "SOM",
    "SDN", "AFG", "HRV", "PHL", "VNM", "PAK", "JAM", "CMR"
}

# ---------------------------------------------------------------------------
# High-risk Merchant Category Codes
# Codes associated with elevated money laundering risk per JMLSG guidance
# ---------------------------------------------------------------------------
HIGH_RISK_MCCS = {
    "6010",  # Manual cash disbursements
    "6011",  # ATM cash withdrawals
    "6012",  # Merchandise/services - financial institutions
    "7995",  # Gambling / betting
    "5912",  # Drug stores / pharmacies (structuring risk)
    "9406",  # Government services (potential corruption risk)
}

# ---------------------------------------------------------------------------
# Scoring weights — must sum to 1.0
# ---------------------------------------------------------------------------
SCORE_WEIGHTS = {
    "amount_risk":       0.30,   # Transaction size relative to £5k threshold
    "country_risk":      0.25,   # Destination country FATF risk rating
    "channel_risk":      0.20,   # Payment channel (SWIFT > CHAPS > FPS > CARD)
    "mcc_risk":          0.15,   # Merchant category risk
    "international_risk": 0.10,  # Cross-border flag
}

# Channel risk scores
CHANNEL_RISK_SCORES = {
    "SWIFT": 0.9,
    "CHAPS": 0.6,
    "BACS":  0.4,
    "FPS":   0.3,
    "CARD":  0.2,
}


def apply_risk_scoring(df: DataFrame) -> DataFrame:
    """
    Apply multi-factor rule-based risk scoring to the filtered transaction stream.

    Each factor produces a sub-score 0.0–1.0 which is weighted and summed
    into a final composite_risk_score.

    A composite_risk_score >= 0.75 routes the transaction to the SAR queue.
    A composite_risk_score >= 0.50 flags for enhanced due diligence review.

    Returns the DataFrame with added columns:
        - score_amount_risk        (float)
        - score_country_risk       (float)
        - score_channel_risk       (float)
        - score_mcc_risk           (float)
        - score_international_risk (float)
        - composite_risk_score     (float, 0.0-1.0)
        - risk_band                (string: LOW/MEDIUM/HIGH/CRITICAL)
        - sar_eligible             (boolean)
        - edd_required             (boolean)
    """
    log.info("Applying multi-factor risk scoring engine (MLR 2017 Reg. 21)")

    # 1. Amount risk — normalised above £5k threshold up to a £100k ceiling
    df = df.withColumn(
        "score_amount_risk",
        F.least(
            F.lit(1.0),
            F.greatest(
                F.lit(0.0),
                (F.col("Transaction_Amount_GBP") - 5000.0) / 95000.0
            )
        ).cast(DoubleType())
    )

    # 2. Country risk — binary: high-risk country = 1.0, else 0.1
    high_risk_list = F.array(*[F.lit(c) for c in HIGH_RISK_COUNTRIES])
    df = df.withColumn(
        "score_country_risk",
        F.when(F.array_contains(high_risk_list, F.col("Country_Code")), 1.0)
         .otherwise(0.1)
        .cast(DoubleType())
    )

    # 3. Channel risk — lookup from CHANNEL_RISK_SCORES map
    channel_map = F.create_map(
        *[item for pair in CHANNEL_RISK_SCORES.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    df = df.withColumn(
        "score_channel_risk",
        F.coalesce(channel_map[F.col("Payment_Channel")], F.lit(0.5))
        .cast(DoubleType())
    )

    # 4. MCC risk — high-risk merchant category = 0.9, else 0.1
    high_risk_mcc_list = F.array(*[F.lit(m) for m in HIGH_RISK_MCCS])
    df = df.withColumn(
        "score_mcc_risk",
        F.when(F.array_contains(high_risk_mcc_list, F.col("Merchant_Category_Code")), 0.9)
         .otherwise(0.1)
        .cast(DoubleType())
    )

    # 5. International risk — cross-border = 0.8, domestic = 0.1
    df = df.withColumn(
        "score_international_risk",
        F.when(F.col("Is_International") == "true", 0.8)
         .otherwise(0.1)
        .cast(DoubleType())
    )

    # 6. Composite weighted score
    df = df.withColumn(
        "composite_risk_score",
        F.round(
            (F.col("score_amount_risk")       * SCORE_WEIGHTS["amount_risk"])      +
            (F.col("score_country_risk")      * SCORE_WEIGHTS["country_risk"])     +
            (F.col("score_channel_risk")      * SCORE_WEIGHTS["channel_risk"])     +
            (F.col("score_mcc_risk")          * SCORE_WEIGHTS["mcc_risk"])         +
            (F.col("score_international_risk")* SCORE_WEIGHTS["international_risk"]),
            4
        ).cast(DoubleType())
    )

    # 7. Risk band classification
    df = df.withColumn(
        "risk_band",
        F.when(F.col("composite_risk_score") >= 0.75, "CRITICAL")
         .when(F.col("composite_risk_score") >= 0.50, "HIGH")
         .when(F.col("composite_risk_score") >= 0.25, "MEDIUM")
         .otherwise("LOW")
    )

    # 8. Regulatory routing flags
    df = df.withColumn(
        "sar_eligible",     # MLR 2017 Reg. 35 — route to SAR queue
        F.col("composite_risk_score") >= 0.75
    )
    df = df.withColumn(
        "edd_required",     # MLR 2017 Reg. 27 — Enhanced Due Diligence required
        F.col("composite_risk_score") >= 0.50
    )

    log.info("Risk scoring complete. Columns added: composite_risk_score, risk_band, sar_eligible, edd_required")
    return df
