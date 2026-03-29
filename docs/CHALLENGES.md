# Challenges & Lessons Learned

Real problems encountered building the ClearPay UK Fraud Detection platform.

---

## 1. Event Hubs Checkpoint Corruption After Databricks Cluster Restart

**The Problem:**
After a Databricks cluster was force-restarted during a maintenance window, the streaming job restarted and began reprocessing events from a checkpoint that was partially written. This caused ~14,000 duplicate transactions to be written to the silver layer — including 23 SAR-eligible events that were double-counted in the compliance dashboard.

**Root Cause:**
The checkpoint was written mid-micro-batch when the cluster was killed. On restart, the streaming engine recovered from the last *fully committed* checkpoint — but the Delta table had already partially received the in-flight batch. Without idempotent writes, this caused duplicates.

**The Fix:**
Migrated the silver write from `append` mode with no deduplication to a `foreachBatch` sink that uses Delta Lake `MERGE` on `Transaction_ID`:

```python
def upsert_to_silver(batch_df, batch_id):
    delta_table.alias("target").merge(
        batch_df.alias("source"),
        "target.Transaction_ID = source.Transaction_ID"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

stream.writeStream.foreachBatch(upsert_to_silver).start()
```

This made all silver writes idempotent — reprocessing the same batch produces the same result.

**Lesson:** Always use `MERGE` (upsert) for streaming Delta writes in production. `append` mode without deduplication is only safe if you can guarantee exactly-once delivery end-to-end — which Event Hubs cannot guarantee across restarts.

---

## 2. SHA-256 Hash Collision Concern for IBAN Pseudonymisation

**The Problem:**
During a compliance review, the legal team raised a concern: SHA-256 without a salt is a *deterministic* hash — meaning an attacker with a list of known UK IBANs (which follow a predictable format: GB + 2 check digits + 4-character bank code + 14-digit account) could theoretically pre-compute a rainbow table of all ~4 billion possible UK IBANs and reverse the hash.

**Root Cause:**
The initial implementation used plain `sha2(iban, 256)` without a salt. UK IBANs have a constrained format, making brute-force pre-computation feasible.

**The Fix:**
Migrated to HMAC-SHA256 using a 32-byte cryptographically random salt stored in Azure Key Vault:

```python
# Salt retrieved from KV at runtime — never hardcoded
iban_salt = dbutils.secrets.get(scope="fintech-kv-scope", key="iban-hash-salt")
F.sha2(F.concat(F.col("Customer_Account_IBAN"), F.lit(iban_salt)), 256)
```

The salt makes pre-computation computationally infeasible without knowing the secret. Documented in the GDPR Data Protection Impact Assessment (DPIA) as a key pseudonymisation control.

**Lesson:** For pseudonymising structured personal data with predictable formats (IBANs, NINOs, postcodes), always use a salted hash. Plain SHA-256 is not sufficient pseudonymisation under UK GDPR Article 25 for guessable data.

---

## 3. Watermark State OOM on Databricks Driver Node

**The Problem:**
In the initial implementation the watermark was set to 48 hours to "be safe" about late-arriving events. After 3 days in production, the Databricks driver node began hitting OOM (out-of-memory) errors and crashing, killing the streaming job.

**Root Cause:**
PySpark Structured Streaming maintains state for all active event-time windows within the watermark period. A 48-hour watermark meant the engine was holding state for 48 hours of high-volume transactions — approximately 115 million records in memory on the driver. This exhausted the 28GB driver node RAM.

**The Fix:**
Analysed 30 days of Event Hubs offset timestamps vs. event timestamps and found:
- 94.1% of events arrive within 2 minutes
- 99.2% within 10 minutes
- 0.8% beyond 10 minutes (SWIFT cross-border outliers)

Reduced the watermark to 10 minutes, which reduced in-memory state by ~288x. The 0.8% of genuinely late events are handled by the nightly Bronze reconciliation batch. Increased the driver node to `Standard_DS5_v2` as an additional buffer.

**Lesson:** Watermark duration is a performance parameter, not just a correctness parameter. Always baseline your late-arrival distribution before setting it. Start small and monitor state store size in Ganglia/Spark UI.

---

## 4. MLR 2017 Threshold Ambiguity — Per-Transaction vs. Per-Customer

**The Problem:**
During a compliance review, an ambiguity emerged: does the £5,000 EDD threshold under MLR 2017 apply per individual transaction, or per customer per day (i.e., structuring detection)?

**Root Cause:**
The regulation text (Reg. 27) refers to "occasional transactions" but the FCA's guidance and JMLSG notes that structuring — splitting large transactions into smaller ones to avoid the threshold — is itself a red flag. Our initial implementation only checked per-transaction amount.

**Resolution:**
Implemented two separate controls:
1. **Per-transaction filter** (this streaming pipeline): Any single transaction >= £5,000 triggers EDD.
2. **Velocity check** (nightly batch job): Customer accounts with cumulative daily volume >= £10,000 across multiple transactions are flagged separately — implemented in the `pl_velocity_check` ADF pipeline.

Documented the legal basis in the DPIA and compliance runbook, signed off by the MLRO.

**Lesson:** Regulatory thresholds in financial services often have nuance that is not obvious from the primary legislation text. Always have your compliance team validate the implementation against FCA/JMLSG guidance, not just the raw statutory instrument.

---

## 5. Dead-Letter Volume Spike Caused by Upstream Schema Change

**The Problem:**
Two weeks after go-live, the dead-letter volume spiked from ~5 events/hour to ~12,000 events/hour. The compliance alert fired immediately (threshold: 10 events/minute).

**Root Cause:**
The upstream payment gateway team had deployed a schema change that renamed `Transaction_Amount_GBP` to `amount_gbp` without coordinating with the data engineering team. Every event from the updated gateway was failing schema validation and landing in the dead-letter queue.

**The Fix:**
1. Immediate: added a schema compatibility layer in the parser to accept both `Transaction_Amount_GBP` and `amount_gbp`:
```python
.withColumn("Transaction_Amount_GBP",
    F.coalesce(F.col("data.Transaction_Amount_GBP"), F.col("data.amount_gbp"))
)
```
2. Longer-term: established a **schema registry** using Azure Schema Registry (Event Hubs premium) so upstream teams must register schema versions before deployment, and the streaming pipeline validates against the registry.

**Lesson:** In event-driven architectures, schema governance between producers and consumers is critical. The dead-letter queue saved us — without it, we would have silently lost 12,000 transactions. Implement a schema registry from day one.
