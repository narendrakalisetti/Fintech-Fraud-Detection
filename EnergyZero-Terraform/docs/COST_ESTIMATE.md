# EnergyZero – Monthly Azure Cost Estimate (UK South, Production)

> Estimates based on Azure Pricing Calculator (April 2025). Actual costs vary by usage.

| Service | Configuration | Est. Monthly Cost (GBP) |
|---|---|---|
| ADLS Gen2 | GRS, 1TB data + 500GB metadata, 10M transactions | ~£28 |
| Azure Data Factory | 1,000 pipeline runs, 500 activity runs, 2 triggers | ~£15 |
| Databricks (Premium) | Standard_DS4_v2, 4 nodes, 8hr/day, auto-terminate 20min | ~£320 |
| Azure Key Vault (Premium) | 10k operations/month, HSM keys | ~£8 |
| Log Analytics | 5GB/day ingest, 90-day retention | ~£68 |
| Virtual Network | Private endpoints (x4), no VPN | ~£12 |
| Microsoft Purview | 1 data map unit, 10 scan runs/month | ~£45 |
| Microsoft Defender for Storage | 5TB scanned | ~£22 |
| **Total** | | **~£518/month** |

## Cost Optimisation Applied

- Databricks clusters auto-terminate after 20 minutes idle (saves ~40% vs always-on)
- ADLS lifecycle policy tiers bronze data to Cool (30d) then Archive (90d) automatically
- Log Analytics 90-day retention minimum (GDPR Art. 30) — no excess retention
- ADF uses event-based triggers only (no polling) — reduces activity runs
- All resources in UK South — Microsoft committed to 100% renewable energy

## Dev Environment Cost

Dev uses LRS storage, Standard Databricks SKU, and 30-day log retention:

| Service | Dev Config | Est. Monthly Cost (GBP) |
|---|---|---|
| ADLS Gen2 | LRS, 100GB | ~£4 |
| Databricks (Standard) | 2 nodes, 4hr/day | ~£45 |
| Key Vault (Standard) | 1k operations | ~£2 |
| Log Analytics | 1GB/day, 30-day retention | ~£8 |
| **Dev Total** | | **~£59/month** |
