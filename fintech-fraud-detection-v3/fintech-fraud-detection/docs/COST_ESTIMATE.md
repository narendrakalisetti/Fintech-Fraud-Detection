# Monthly Cost Estimate — ClearPay UK Fraud Detection (Production)

Estimates based on Azure Pricing Calculator (March 2026, UK South, Pay-as-you-go).

| Service | Configuration | Est. Monthly (GBP) |
|---|---|---|
| Azure Event Hubs | Standard tier, 10 Throughput Units, 2.4M events/day | 48 |
| Databricks (streaming 24/7) | Standard_DS4_v2, 4 nodes, spot with fallback | 310 |
| Databricks (gold notebook) | Standard_DS3_v2, 2 nodes, 1hr/day scheduled | 22 |
| ADLS Gen2 (bronze+silver+gold) | LRS, ~500GB, Standard tier | 9 |
| ADLS operations | ~50M read/write ops/month | 8 |
| Azure Key Vault | Standard SKU, ~20k operations/month | 3 |
| Log Analytics | 3GB/day ingestion, 90-day retention | 18 |
| Azure Monitor Alerts | 5 alert rules | 2 |
| Total | | ~420/month |

## Cost Optimisation Applied

- Databricks spot instances with fallback reduce compute cost ~40% vs. on-demand
- Cluster auto-terminates after 20 min idle (cluster policy enforced)
- Bronze lifecycle: cool after 30 days, archive after 60 days
- Log Analytics 90-day retention (not the 730-day default)
- Event Hubs auto-inflate disabled — fixed 10 TUs, manually scale if needed

## Dev Environment

Single-node Databricks, LRS storage, standard Key Vault, 7-day log retention:
**~85 GBP/month**
