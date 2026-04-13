# Changelog — EnergyZero-Terraform

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [2.0.0] — 2025-04-13

### Added
- Microsoft Purview module (`modules/purview/`) — was referenced in architecture diagram but missing from codebase
- RBAC assignment: Purview managed identity → ADLS Gen2 Storage Blob Data Reader
- `outputs.tf`: `purview_account_id`, `catalog_endpoint`, `gold_layer_dfs_endpoint`, `databricks_sql_warehouse_endpoint`
- Power BI DirectQuery setup instructions in README (wired to `outputs.tf`)
- PySpark unit tests in `/tests/` — 15 tests covering hash, cleanse, metadata, PII drop
- `conftest.py` and `pytest.ini` for test suite configuration
- `requirements.txt` and `requirements-dev.txt`
- `environments/dev/terraform.tfvars` differentiated from prod (LRS, Standard Databricks, 30d logs)
- `docs/CHALLENGES.md` — 7 documented engineering challenges with solutions
- `docs/COST_ESTIMATE.md` — accurate monthly cost breakdown (~£518/month prod, ~£59/month dev)
- Delta maintenance: OPTIMIZE + ZORDER + VACUUM added to PySpark notebooks
- `scripts/smoke_test.sh` — post-deploy pipeline smoke test

### Changed
- **BREAKING**: AzureRM provider upgraded from `~> 3.110` to `~> 4.0`
- **BREAKING**: `azurerm_storage_container` now uses `storage_account_id` (not `storage_account_name`)
- Terraform required version bumped from `>= 1.8.0` to `>= 1.9.0`
- AzureAD provider upgraded from `~> 2.53` to `~> 3.0`
- Key Vault SKU upgraded to `premium` for HSM-backed CMK (was `standard`)
- CMK key type upgraded to `RSA-HSM 4096` with auto-rotation policy (365 days)
- GitHub Actions: added `concurrency` group to prevent state lock conflicts
- README: removed broken screenshot table; replaced with accurate Power BI setup guide
- README: Purview section updated to reflect real provisioned module
- Cost estimate corrected from ~£198/month to ~£518/month (Databricks was severely understated)

### Fixed
- Purview module now exists in codebase — was in architecture diagram only
- CI/CD badges now point to actual workflow file at `.github/workflows/terraform.yml`
- `environments/dev/` and `environments/prod/` are now properly differentiated
- Architecture diagram updated to remove dangling Power BI arrow (now wired)

## [1.0.0] — 2024-11-01

### Added
- Initial release: ADLS Gen2 medallion architecture, Key Vault CMK, ADF, Databricks, networking, monitoring
- Bronze/silver/gold containers with lifecycle policies
- VNet injection, private endpoints, NSGs
- GitHub Actions CI: fmt, validate, tflint, checkov, plan, apply
- ADF pipelines: `pl_ingest_ofgem_bronze.json`
- PySpark notebooks: `01_bronze_to_silver.py`, `02_silver_to_gold.py`
- SQL gold views
- Sample Ofgem data (50 rows)
