# Changelog — Fintech-Fraud-Detection

## [2.0.0] — 2025-04-13

### Fixed (Critical)
- **CI path bug**: Workflow files moved from `.github/workflows/workflows/` to `.github/workflows/` — GitHub Actions now correctly detects and runs CI
- **Consumer group**: Replaced `$Default` with dedicated `fraud-detection-streaming-job` consumer group — eliminates offset conflicts between streaming jobs
- **Checkpoint location**: Added persistent ADLS Gen2 checkpoint path — streaming job now resumes from correct offset after cluster restart (no more full replays)
- **Explicit schema**: Added `TRANSACTION_SCHEMA` `StructType` — schema mismatches now fail fast to dead-letter instead of silently corrupting downstream aggregations
- **Coverage badge**: Removed static hardcoded `Coverage-92%` badge (was fabricated) — badge now reflects actual CI run output
- **Floating-point modulo**: `score_large_round_amount` now casts to `LongType` before `% 1000` — eliminates floating-point precision errors in risk scoring

### Added
- `tests/test_fraud_detection.py` — 20 unit tests (schema, PII hash, filter, watermark)
- `tests/test_risk_scoring.py` — 15 unit tests (all 5 risk factors, composite score, SAR routing)
- `tests/test_dead_letter.py` — 6 unit tests (failure classification, review status)
- `tests/test_integration.py` — 6 end-to-end pipeline integration tests
- `conftest.py` at repo root — shared Spark session fixture
- `scripts/generate_sample_data.py` — generates 100,000+ synthetic UK transactions with realistic distributions (high-value ~8%, international ~15%, high-risk country ~1%)
- `docs/CHALLENGES.md` — 7 real engineering problems with solutions
- `docs/COST_ESTIMATE.md` — corrected Databricks cost (~£980–1,118/month vs. previous £310)
- `notebooks/01_silver_to_gold.py` — added Delta OPTIMIZE + ZORDER + VACUUM for small file management
- `sql/gold_views.sql` — added `vw_executive_summary` 24-hour rolling view
- `configs/risk_rules.json` — externalised risk weights and MLR 2017 parameters

### Changed
- Sample data: `sample_transactions.csv` kept at 50 rows for quick smoke test; use `generate_sample_data.py` for load testing
- README: accurate cost figures, documented consumer group and checkpoint requirements, removed static coverage badge
- `setup.cfg`: coverage `fail_under = 85` enforced in CI

## [1.0.0] — 2024-10-15

### Added
- Initial release: PySpark Structured Streaming pipeline, risk scoring engine, dead-letter handler
- Azure Event Hubs ingestion, ADLS Gen2 Delta Lake medallion
- MLR 2017 SAR routing, UK GDPR PII hashing
- Databricks job config, ADF-compatible architecture
- GitHub Actions CI/CD, pre-commit hooks
