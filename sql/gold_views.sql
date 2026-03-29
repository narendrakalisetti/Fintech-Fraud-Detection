-- =============================================================================
-- ClearPay UK | Gold Layer SQL Views
-- =============================================================================
-- Purpose   : Databricks SQL views over Delta Lake gold tables for
--             Power BI DirectQuery and compliance analyst queries.
-- GDPR      : No raw PII in any view. Hashed IBANs in sar_queue only.
-- MLR 2017  : sar_pending view supports MLRO daily review workflow.
-- Author    : Narendra Kalisetti
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. vw_fraud_daily_summary
-- Daily fraud detection KPIs — top-level Power BI dashboard cards
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW clearpay_gold.vw_fraud_daily_summary AS
SELECT
    Silver_Load_Date                                        AS report_date,
    total_flagged_transactions,
    ROUND(total_flagged_amount_gbp, 2)                     AS total_flagged_amount_gbp,
    ROUND(avg_risk_score, 4)                               AS avg_risk_score,
    sar_eligible_total,
    edd_required_total,
    critical_risk_count,
    high_risk_count,
    ROUND(fraud_detection_rate_pct, 2)                     AS fraud_detection_rate_pct,
    ROUND(international_amount_gbp, 2)                     AS international_amount_gbp,
    -- Week-over-week change in SAR volume
    sar_eligible_total - LAG(sar_eligible_total, 7)
        OVER (ORDER BY Silver_Load_Date)                   AS sar_wow_change,
    -- 7-day rolling average flagged transactions
    AVG(total_flagged_transactions)
        OVER (ORDER BY Silver_Load_Date
              ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)   AS rolling_7d_avg_flagged,
    gold_load_timestamp
FROM delta.`abfss://gold@{storage_account}.dfs.core.windows.net/executive_fraud_summary/`
WHERE Silver_Load_Date >= DATEADD(DAY, -90, CURRENT_DATE())
ORDER BY Silver_Load_Date DESC;

-- ---------------------------------------------------------------------------
-- 2. vw_fraud_by_risk_band
-- Risk band breakdown — Power BI donut chart and bar chart
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW clearpay_gold.vw_fraud_by_risk_band AS
SELECT
    Silver_Load_Date                                        AS report_date,
    risk_band,
    risk_band_label,
    Payment_Channel,
    Country_Code,
    transaction_count,
    ROUND(total_amount_gbp, 2)                             AS total_amount_gbp,
    ROUND(avg_amount_gbp, 2)                               AS avg_amount_gbp,
    ROUND(max_amount_gbp, 2)                               AS max_amount_gbp,
    ROUND(avg_risk_score, 4)                               AS avg_risk_score,
    sar_eligible_count,
    edd_required_count,
    ROUND(sar_rate_pct, 2)                                 AS sar_rate_pct,
    international_count,
    gold_load_timestamp
FROM delta.`abfss://gold@{storage_account}.dfs.core.windows.net/fraud_kpis/`
WHERE Silver_Load_Date >= DATEADD(DAY, -30, CURRENT_DATE());

-- ---------------------------------------------------------------------------
-- 3. vw_sar_pending_review
-- SAR-eligible transactions pending MLRO review
-- MLR 2017 Reg.35: MLRO reviews within 1 hour; SAR submitted within 7 days
-- NOTE: Contains hashed IBAN for MLRO cross-reference only
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW clearpay_gold.vw_sar_pending_review AS
SELECT
    Transaction_ID,
    Event_Timestamp,
    Silver_Load_Date,
    Customer_Account_IBAN_Hashed                           AS sender_iban_hash,
    Beneficiary_IBAN_Hashed                                AS beneficiary_iban_hash,
    ROUND(Transaction_Amount_GBP, 2)                       AS amount_gbp,
    Payment_Channel,
    Country_Code,
    Is_International,
    ROUND(composite_risk_score, 4)                         AS risk_score,
    risk_band,
    Merchant_Category_Code,
    sar_review_status,
    sar_submission_deadline,
    mlr_regulation,
    -- Days remaining until SAR submission deadline
    DATEDIFF(sar_submission_deadline, CURRENT_DATE())      AS days_to_deadline,
    -- Urgency flag for Power BI conditional formatting
    CASE
        WHEN DATEDIFF(sar_submission_deadline, CURRENT_DATE()) <= 1 THEN 'URGENT'
        WHEN DATEDIFF(sar_submission_deadline, CURRENT_DATE()) <= 3 THEN 'DUE_SOON'
        ELSE 'ON_TRACK'
    END                                                    AS deadline_urgency
FROM delta.`abfss://gold@{storage_account}.dfs.core.windows.net/sar_queue/`
WHERE sar_review_status = 'PENDING_MLRO_REVIEW'
ORDER BY composite_risk_score DESC, Event_Timestamp ASC;

-- ---------------------------------------------------------------------------
-- 4. vw_high_risk_channels
-- Payment channel risk analysis — used by risk team for channel controls
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW clearpay_gold.vw_high_risk_channels AS
SELECT
    Silver_Load_Date                                        AS report_date,
    Payment_Channel,
    SUM(transaction_count)                                 AS total_transactions,
    SUM(sar_eligible_count)                                AS sar_eligible,
    ROUND(SUM(total_amount_gbp), 2)                        AS total_amount_gbp,
    ROUND(AVG(avg_risk_score), 4)                          AS avg_risk_score,
    ROUND(SUM(sar_eligible_count) * 100.0 /
          NULLIF(SUM(transaction_count), 0), 2)            AS sar_rate_pct,
    -- Channel risk ranking
    RANK() OVER (
        PARTITION BY Silver_Load_Date
        ORDER BY AVG(avg_risk_score) DESC
    )                                                      AS risk_rank
FROM delta.`abfss://gold@{storage_account}.dfs.core.windows.net/fraud_kpis/`
WHERE Silver_Load_Date >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY Silver_Load_Date, Payment_Channel
ORDER BY Silver_Load_Date DESC, avg_risk_score DESC;

-- ---------------------------------------------------------------------------
-- 5. Data freshness check — run after each gold load
-- ---------------------------------------------------------------------------
-- SELECT
--     MAX(Silver_Load_Date)      AS latest_load_date,
--     MAX(gold_load_timestamp)   AS latest_load_timestamp,
--     COUNT(*)                   AS total_sar_pending,
--     SUM(CASE WHEN days_to_deadline <= 1 THEN 1 ELSE 0 END) AS urgent_count
-- FROM clearpay_gold.vw_sar_pending_review;
