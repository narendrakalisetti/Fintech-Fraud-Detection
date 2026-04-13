# EnergyZero – Azure Data Engineering Platform (Terraform IaC)

![Terraform CI](https://github.com/narendrakalisetti/EnergyZero-Terraform/actions/workflows/terraform.yml/badge.svg)
![Python Tests](https://github.com/narendrakalisetti/EnergyZero-Terraform/actions/workflows/terraform.yml/badge.svg)
![Terraform](https://img.shields.io/badge/Terraform-1.9-purple)
![AzureRM](https://img.shields.io/badge/AzureRM-4.x-blue)
![Azure](https://img.shields.io/badge/Azure-UK%20South-blue)
![GDPR](https://img.shields.io/badge/GDPR-Compliant-success)
![Net Zero](https://img.shields.io/badge/Net%20Zero-2050%20Aligned-brightgreen)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.1-orange)

---

## Business Context

**EnergyZero** is a UK-based renewable energy company tracking smart meter readings, grid voltage metrics, and carbon output data from thousands of IoT sensors across the UK national grid. Their data team needed a cloud-native, production-grade data platform that could:

- Ingest raw sensor telemetry and Ofgem regulatory CSV reports into a central data lake
- Clean, validate, and transform the data into analytics-ready layers
- Enforce GDPR compliance for any customer-linked energy consumption records
- Support Net Zero 2050 reporting — tracking renewable output vs. carbon targets per region
- Be fully auditable, secure, and operable by a lean data engineering team of four

This repository provisions the **entire Azure data engineering infrastructure** using Terraform IaC and includes working ADF pipelines, PySpark transformation notebooks, PySpark unit tests, SQL gold-layer views, and sample data.

---

## Architecture

```
graph TD
    subgraph Sources
        A[Ofgem CSV Reports]
        B[Smart Meter IoT / Event Hubs]
        C[Weather API]
    end

    subgraph Orchestration
        D[Azure Data Factory\nadf-uks-energyzero-prod]
    end

    subgraph ADLS Gen2 - Medallion Architecture
        E[bronze/ Raw ingestion]
        F[silver/ Cleansed + validated + Delta]
        G[gold/ Aggregated BI-ready Delta]
    end

    subgraph Compute
        H[Azure Databricks\nPySpark Notebooks]
    end

    subgraph Security
        I[Azure Key Vault\nSecrets + CMK]
    end

    subgraph Monitoring
        J[Log Analytics\nAlerts + KQL Dashboards]
    end

    subgraph Governance
        K[Microsoft Purview\nData Catalogue + Lineage]
    end

    subgraph Visualisation
        L[Power BI\nDirectQuery via Databricks SQL Warehouse]
    end

    A --> D --> E --> H --> F --> H --> G
    G --> L
    I -.-> D
    I -.-> H
    D --> J
    H --> J
    K -.->|Scans| E
    K -.->|Scans| F
    K -.->|Scans| G
```

---

## What Gets Deployed

| Resource | Name | Purpose |
|---|---|---|
| Resource Group | `rg-uks-energyzero-prod` | Container for all resources |
| ADLS Gen2 | `sauksenergyzeroprod` | Bronze / Silver / Gold Delta Lake |
| Azure Key Vault | `kv-uks-energyzero-prod` | Secrets, CMK (HSM-backed RSA-4096), IBAN salt |
| Azure Data Factory | `adf-uks-energyzero-prod` | Pipeline orchestration |
| Databricks Workspace | `dbw-uks-energyzero-prod` | PySpark transformation jobs |
| Log Analytics | `law-uks-energyzero-prod` | Centralised monitoring + KQL alerts |
| Microsoft Purview | `purview-uks-energyzero-prod` | Data catalogue, lineage, classification |
| Terraform State SA | `sauksenergyzerotfstate` | Remote state + locking |

---

## Medallion Architecture — Data Flow

```
bronze/ofgem/           ← Raw Ofgem CSV (unchanged, partitioned by ingest date)
bronze/smartmeter/      ← Raw IoT JSON payloads from Event Hubs
bronze/weather/         ← Raw weather API JSON

silver/meter_readings/  ← Cleansed, typed, deduplicated Delta table
                          customer_id hashed SHA-256+salt (GDPR Art. 25)
silver/grid_metrics/    ← Validated grid metrics
silver/weather_clean/   ← Joined weather enrichment

gold/net_zero_summary/  ← Regional renewable output vs. carbon targets
gold/grid_kpis/         ← Daily grid voltage stability KPIs
gold/executive_report/  ← Pre-aggregated for Power BI DirectQuery
```

---

## Security Architecture

```
┌──────────────────────────────────────────────────────────┐
│  No public access to ADLS or Key Vault in production     │
│  All traffic via Private Endpoints + VNet injection      │
│                                                          │
│  ADF Managed Identity ──RBAC──► ADLS Gen2                │
│  ADF Managed Identity ──RBAC──► Key Vault (Secrets User) │
│  Databricks MI        ──RBAC──► ADLS Gen2                │
│  Databricks MI        ──RBAC──► Key Vault (Secrets User) │
│  Purview MI           ──RBAC──► ADLS Gen2 (Reader only)  │
│                                                          │
│  All secrets : Key Vault (never in state / code)         │
│  Encryption  : AES-256 at rest, TLS 1.2+ in transit     │
│  CMK         : HSM-backed RSA-4096, auto-rotate 365 days │
│  Soft-delete : 90 days (Key Vault) / 30 days (ADLS)     │
│  Purge protect: Enabled (GDPR Art. 32 availability)     │
└──────────────────────────────────────────────────────────┘
```

---

## GDPR Compliance

| UK GDPR Requirement | Implementation |
|---|---|
| Art. 5 – Data minimisation | Only necessary fields stored; raw PII masked at silver |
| Art. 25 – Privacy by Design | Customer IDs hashed SHA-256+salt before silver write |
| Art. 32 – Security of processing | CMK encryption, private endpoints, RBAC least-privilege |
| Art. 17 – Right to Erasure | Bronze soft-delete 30d; silver/gold TTL policies |
| Art. 30 – Records of processing | All pipeline runs logged to Log Analytics (90-day retention) |
| Art. 4(7) – Data Controller tag | `DataController = EnergyZero Ltd` on all resources |

---

## Power BI DirectQuery Setup

After deployment, connect Power BI to the gold layer via Databricks SQL Warehouse:

1. In Databricks workspace → **SQL Warehouses** → create or start a warehouse
2. Copy the **Server hostname** and **HTTP path** from Connection Details
3. In Power BI Desktop → **Get Data** → **Azure Databricks**
4. Enter the server hostname and HTTP path
5. Authenticate via Azure AD (same managed identity flow)
6. Select tables from `gold.vw_net_zero_progress` or `gold.vw_executive_dashboard`

The `outputs.tf` exports `databricks_sql_warehouse_endpoint` and `gold_layer_dfs_endpoint` for reference.

---

## Project Structure

```
EnergyZero-Terraform/
├── main.tf                         # Root module — all module calls + RBAC
├── variables.tf                    # Input variables with validation
├── locals.tf                       # Computed values + common tags
├── outputs.tf                      # Exposed values (Power BI endpoints etc.)
│
├── modules/
│   ├── adls/                       # ADLS Gen2 + containers + lifecycle + private endpoints
│   ├── keyvault/                   # Key Vault + CMK (HSM RSA-4096) + RBAC
│   ├── adf/                        # Data Factory + linked services + diagnostics
│   ├── databricks/                 # Databricks workspace + VNet injection
│   ├── networking/                 # VNet + subnets + NSGs + private endpoints
│   ├── monitoring/                 # Log Analytics + alerts + action groups
│   └── purview/                    # Microsoft Purview account + managed identity
│
├── adf-pipelines/
│   └── pl_ingest_ofgem_bronze.json # Copy Activity + Databricks trigger chain
│
├── notebooks/
│   ├── 01_bronze_to_silver.py      # PySpark: cleanse + hash PII + Delta MERGE
│   └── 02_silver_to_gold.py        # PySpark: Net Zero KPIs + grid stability
│
├── tests/
│   ├── conftest.py                 # Shared pytest fixtures
│   └── test_bronze_to_silver.py    # 15 unit tests for transformation functions
│
├── sql/
│   └── gold_views.sql              # Power BI DirectQuery views
│
├── sample_data/
│   └── ofgem_sample.csv            # 50-row synthetic Ofgem data
│
├── environments/
│   ├── prod/terraform.tfvars       # Production: GRS, premium Databricks, 90d logs
│   └── dev/terraform.tfvars        # Dev: LRS, standard Databricks, 30d logs
│
├── docs/
│   ├── CHALLENGES.md               # 7 real engineering problems + solutions
│   └── COST_ESTIMATE.md            # Accurate monthly cost breakdown (~£518/month)
│
├── scripts/
│   ├── bootstrap_state.sh          # One-time remote state setup
│   ├── deploy_adf_pipelines.sh     # CI/CD ADF pipeline publish
│   └── smoke_test.sh               # Post-deploy pipeline smoke test
│
├── .github/workflows/
│   └── terraform.yml               # fmt + validate + tflint + checkov + plan + apply
│
├── .pre-commit-config.yaml
├── .tflint.hcl
├── requirements.txt
├── requirements-dev.txt
├── pytest.ini
├── CHANGELOG.md
└── README.md
```

---

## Quick Start

### Prerequisites
- Azure CLI installed and `az login` completed
- Terraform >= 1.9.0
- Contributor + User Access Administrator on an Azure subscription
- Python 3.11+ (for running PySpark unit tests locally)

### 1 — Bootstrap Remote State

```bash
bash scripts/bootstrap_state.sh
```

### 2 — Run PySpark Unit Tests Locally

```bash
pip install -r requirements-dev.txt
pytest tests/ -v --cov=notebooks --cov-report=term-missing
```

### 3 — Init and Plan

```bash
terraform init \
  -backend-config="resource_group_name=rg-uks-energyzero-tfstate" \
  -backend-config="storage_account_name=sauksenergyzerotfstate" \
  -backend-config="container_name=tfstate" \
  -backend-config="key=prod/energyzero.tfstate"

terraform plan -var-file=environments/prod/terraform.tfvars -out=tfplan
```

### 4 — Apply

```bash
terraform apply tfplan
```

### 5 — Deploy ADF Pipelines + Smoke Test

```bash
bash scripts/deploy_adf_pipelines.sh
bash scripts/smoke_test.sh
```

---

## CI/CD Pipeline

```
Pull Request →  terraform fmt -check
                terraform validate
                tflint
                checkov (Bridgecrew security scan → SARIF)
                terraform plan

Merge to main → terraform apply
                deploy ADF pipelines (with 10-min RBAC propagation wait)
                smoke test pipeline run
```

---

## Challenges & Lessons Learned

See [`docs/CHALLENGES.md`](docs/CHALLENGES.md) — covers 7 real problems including RBAC propagation delays, Key Vault soft-delete conflicts, AzureRM 4.x breaking changes, and Delta OPTIMIZE permission requirements.

---

## Cost Estimate

See [`docs/COST_ESTIMATE.md`](docs/COST_ESTIMATE.md) — accurate breakdown totalling ~£518/month for production, ~£59/month for dev.

---

*Built by Narendra Kalisetti · MSc Applied Data Science, Teesside University*
*Portfolio: [narendrakalisetti.vercel.app](https://narendrakalisetti.vercel.app) · LinkedIn: [linkedin.com/in/narendra-kalisetti](https://linkedin.com/in/narendra-kalisetti)*
