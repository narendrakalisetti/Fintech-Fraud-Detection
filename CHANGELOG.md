# Changelog

## [2.1.0] - 2026-03-29
### Added
- Risk scoring engine (risk_scoring.py) — 5-factor weighted composite score
- Dead-letter handler (dead_letter_handler.py) — malformed event capture
- Silver-to-gold notebook (01_silver_to_gold.py) — fraud KPIs + SAR queue
- Gold SQL views (4 views for Power BI DirectQuery)
- SAR queue write stream (MLR 2017 Reg.35 compliance)
- Bronze write stream (full audit trail for all transactions)
- Integration test suite (test_integration.py)
- Dead-letter test suite (test_dead_letter.py)
- Risk scoring test suite (test_risk_scoring.py)
- Databricks job config (job_config.json)
- Security scan CI workflow (bandit + pip-audit)
- Pre-commit hooks (black, flake8, bandit)
- 50-row sample data with FATF high-risk country transactions
- Malformed event samples for dead-letter testing
- CHALLENGES.md — 5 real production problems + solutions
- COST_ESTIMATE.md — full monthly breakdown
- CONTRIBUTING.md

## [1.0.0] - 2026-01-15
### Added
- Initial streaming pipeline (Event Hubs -> Silver Delta)
- IBAN SHA-256 hashing (GDPR Art.25)
- High-value filter (MLR 2017 threshold)
- Watermarking strategy (10-minute window)
- Basic unit tests
- GitHub Actions CI
