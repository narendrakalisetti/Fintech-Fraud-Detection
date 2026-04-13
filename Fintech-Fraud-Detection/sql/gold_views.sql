-- sql/gold_views.sql
-- Power BI DirectQuery views over Delta Lake gold layer.
-- Run in Databricks SQL Warehouse after silver→gold job completes.

-- ============================================================
-- View: Fraud KPI Dashboard (hourly buckets)
-- ============================================================
CREATE OR REPLACE VIEW gold.vw_fraud_kpis AS
SELECT
    hour_bucket,
    transaction_type,
    risk_label,
    transaction_count,
    ROUND(total_amount_gbp, 2)   AS total_amount_gbp,
    ROUND(avg_amount_gbp, 2)     AS avg_amount_gbp,
    ROUND(max_amount_gbp, 2)     AS max_amount_gbp,
    ROUND(avg_risk_score, 4)     AS avg_risk_score,
    sar_count,
    calculated_at,
    -- RAG status for dashboard
    CASE
        WHEN avg_risk_score >= 0.6 THEN 'Red'
        WHEN avg_risk_score >= 0.4 THEN 'Amber'
        ELSE 'Green'
    END AS rag_status
FROM delta.`abfss://gold@sauksfraudprod.dfs.core.windows.net/fraud_kpis/`
ORDER BY hour_bucket DESC;

-- ============================================================
-- View: SAR Queue (compliance output for NCA submission)
-- ============================================================
CREATE OR REPLACE VIEW gold.vw_sar_queue AS
SELECT
    transaction_id,
    iban_hash,
    counterparty_iban_hash,
    ROUND(amount_gbp, 2)   AS amount_gbp,
    transaction_type,
    merchant_category,
    country_code,
    event_timestamp,
    ROUND(risk_score, 4)   AS risk_score,
    risk_label,
    sar_triggered_at,
    sar_status
FROM delta.`abfss://gold@sauksfraudprod.dfs.core.windows.net/sar_queue/`
WHERE sar_status = 'PENDING_NCA_REVIEW'
ORDER BY sar_triggered_at DESC;

-- ============================================================
-- View: Dead-Letter Queue (ops review)
-- ============================================================
CREATE OR REPLACE VIEW gold.vw_dead_letter AS
SELECT
    transaction_id,
    failure_category,
    review_status,
    captured_at
FROM delta.`abfss://gold@sauksfraudprod.dfs.core.windows.net/dead_letter/`
WHERE review_status = 'PENDING'
ORDER BY captured_at DESC;

-- ============================================================
-- View: 24-hour Rolling Summary (executive dashboard)
-- ============================================================
CREATE OR REPLACE VIEW gold.vw_executive_summary AS
SELECT
    DATE(hour_bucket)                     AS report_date,
    SUM(transaction_count)                AS total_transactions,
    ROUND(SUM(total_amount_gbp), 2)       AS total_value_gbp,
    SUM(sar_count)                        AS total_sar_events,
    ROUND(SUM(sar_count) * 100.0
          / NULLIF(SUM(transaction_count), 0), 4) AS sar_rate_pct,
    ROUND(AVG(avg_risk_score), 4)         AS avg_risk_score,
    MAX(max_amount_gbp)                   AS max_single_txn_gbp
FROM gold.vw_fraud_kpis
WHERE hour_bucket >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
GROUP BY DATE(hour_bucket)
ORDER BY report_date DESC;
