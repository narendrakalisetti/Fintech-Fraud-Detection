# Fintech Fraud Detection – Monthly Azure Cost Estimate (UK South, Production)

> Estimates based on Azure Pricing Calculator (April 2025). Actual costs vary with Event Hubs throughput and Databricks DBU consumption.

## Production Cost Breakdown

| Service | Configuration | Est. Monthly Cost (GBP) |
|---|---|---|
| Azure Event Hubs | Standard, 10 TUs, ~2.4M events/day (72M/month) | ~£68 |
| Databricks (Premium) | Standard_DS4_v2, 4 nodes, 24/7 continuous streaming | ~£980 |
| ADLS Gen2 | LRS, ~500GB data, 50M transactions/month | ~£14 |
| Log Analytics | 3GB/day ingest, 90-day retention | ~£45 |
| Azure Key Vault | Standard, 20k ops/month | ~£3 |
| Azure Monitor Alerts | 5 alert rules, PagerDuty integration | ~£8 |
| **Total** | | **~£1,118/month** |

> **Note on Databricks:** A 4-node DS4_v2 cluster running 24/7 on Databricks Premium tier in UK South costs approximately £0.30–£0.35/DBU × 16 DBUs × 730 hours = ~£3,500–4,000/month at list price. The £980 estimate assumes 70% spot instance coverage (SPOT_WITH_FALLBACK_AZURE), which is realistic but not guaranteed. Budget conservatively at ~£1,500–2,000/month with spot.

## Cost Optimisation Applied

- **Spot instances**: `SPOT_WITH_FALLBACK_AZURE` configured in job cluster — reduces Databricks cost ~60–70% vs on-demand
- **Dedicated streaming cluster**: Sized for sustained 2.4M events/day throughput — no over-provisioning
- **ADLS LRS**: Fraud data is derived from payment systems (source of truth elsewhere) — LRS sufficient, no GRS needed
- **Event Hubs auto-inflate disabled**: Fixed 10 TUs based on observed peak throughput — avoids unexpected scaling costs

## Dev Environment Cost

| Service | Dev Config | Est. Monthly Cost (GBP) |
|---|---|---|
| Event Hubs | Basic, 1 TU, test events only | ~£9 |
| Databricks | 2-node DS3_v2, manual start only | ~£45 |
| ADLS Gen2 | LRS, 50GB | ~£2 |
| Log Analytics | 1GB/day, 30-day retention | ~£8 |
| **Dev Total** | | **~£64/month** |
