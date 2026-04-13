-- gold_views.sql
-- Power BI DirectQuery views over Delta Lake gold layer.
-- Run in Databricks SQL or Synapse Analytics.

-- ============================================================
-- View: Net Zero 2050 Regional Progress
-- ============================================================
CREATE OR REPLACE VIEW gold.vw_net_zero_progress AS
SELECT
    report_date,
    region_code,
    total_kwh,
    renewable_kwh,
    renewable_share_pct,
    net_zero_gap_pct,
    reading_count,
    calculated_at,
    -- RAG status for dashboard KPIs
    CASE
        WHEN renewable_share_pct >= 80 THEN 'Green'
        WHEN renewable_share_pct >= 50 THEN 'Amber'
        ELSE 'Red'
    END AS rag_status
FROM delta.`abfss://gold@sauksenergyzeroprod.dfs.core.windows.net/net_zero_summary/`
ORDER BY report_date DESC, region_code;

-- ============================================================
-- View: Grid Voltage Stability KPIs
-- ============================================================
CREATE OR REPLACE VIEW gold.vw_grid_stability AS
SELECT
    report_date,
    region_code,
    avg_voltage,
    min_voltage,
    max_voltage,
    ROUND(voltage_stddev, 4)     AS voltage_stddev,
    under_voltage_events,
    over_voltage_events,
    total_readings,
    ROUND(stability_score * 100, 2) AS stability_score_pct,
    calculated_at,
    CASE
        WHEN stability_score >= 0.99 THEN 'Stable'
        WHEN stability_score >= 0.95 THEN 'Minor Issues'
        ELSE 'Critical'
    END AS grid_status
FROM delta.`abfss://gold@sauksenergyzeroprod.dfs.core.windows.net/grid_kpis/`
ORDER BY report_date DESC, region_code;

-- ============================================================
-- View: Executive Dashboard (30-day rolling)
-- ============================================================
CREATE OR REPLACE VIEW gold.vw_executive_dashboard AS
SELECT
    report_date,
    region_code,
    total_kwh,
    renewable_share_pct,
    net_zero_gap_pct,
    stability_score_pct,
    grid_status,
    rag_status
FROM gold.vw_net_zero_progress nz
LEFT JOIN gold.vw_grid_stability gs USING (report_date, region_code)
WHERE report_date >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY report_date DESC, region_code;
