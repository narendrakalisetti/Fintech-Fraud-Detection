# Fintech Fraud Detection – Engineering Challenges & Solutions

## 1. Event Hubs Consumer Group Conflict

**Problem:** Initial deployment used the `$Default` consumer group. When a second Spark job (silver→gold batch) was started, it competed for the same partition offsets. The streaming job started seeing duplicate events and then lost its offset position entirely, requiring a full replay from the earliest offset.

**Solution:** Created a dedicated consumer group `fraud-detection-streaming-job` in the Event Hubs configuration. Each Structured Streaming job that reads from Event Hubs must have its own consumer group. Consumer groups are cheap — there's no reason to share `$Default`.

```python
# WRONG — causes offset conflicts
"eventhubs.consumerGroup": "$Default"

# CORRECT — dedicated consumer group per job
"eventhubs.consumerGroup": "fraud-detection-streaming-job"
```

**Lesson:** Never use `$Default` in production streaming jobs. Always create dedicated consumer groups. Azure Event Hubs Standard tier supports up to 20 consumer groups per hub.

---

## 2. Checkpoint Location Not Configured — Caused Full Replay on Restart

**Problem:** The initial streaming job had no checkpoint location configured. When the Databricks cluster was restarted after a driver node failure, the job replayed all events from the earliest offset in Event Hubs (72-hour retention). This caused 1.2M duplicate SAR events being written to the gold queue, triggering false NCA alerts.

**Solution:** Added a persistent checkpoint location on ADLS Gen2:

```python
.option("checkpointLocation", "abfss://bronze@sauksfraudprod.dfs.core.windows.net/_checkpoints/fraud_stream")
```

The checkpoint stores the last committed offset per partition. On restart, the job resumes from exactly where it left off. Delta Lake's exactly-once semantics ensure no duplicates even on partial micro-batch failures.

**Lesson:** Checkpoint location is not optional for production streaming jobs. Always store on durable storage (ADLS, not DBFS `/tmp`). Test job restart behaviour before go-live.

---

## 3. Schema Inference Silently Corrupted Stream

**Problem:** Without an explicit schema, PySpark inferred the schema from the first micro-batch. A payment gateway upgrade changed `amount_gbp` from `DOUBLE` to `STRING` (formatted as `"12,500.00"` with comma separators). Schema inference on the next micro-batch failed silently — the amount column became null for all rows. SAR detection stopped working for 4 hours before monitoring caught it.

**Solution:** Defined an explicit `TRANSACTION_SCHEMA` using `StructType`. Schema mismatches now fail fast and route to the dead-letter handler rather than silently poisoning downstream aggregations.

**Lesson:** Never rely on schema inference for production streaming pipelines. Define and own your schema explicitly. Use `mode("FAILFAST")` or dead-letter routing for schema violations.

---

## 4. `.github/workflows/workflows/` Double-Nested Folder

**Problem:** GitHub Actions only reads workflow files from `.github/workflows/*.yml`. The initial repository accidentally had files at `.github/workflows/workflows/ci.yml` — the extra nesting meant GitHub never detected the workflows. The CI badges showed "never run" and recruiters questioned whether CI was actually configured.

**Solution:** Moved all workflow files to `.github/workflows/ci.yml` and `.github/workflows/security.yml` directly. Verified badges turned green after the first push.

**Lesson:** GitHub Actions path is non-negotiable: `.github/workflows/your-workflow.yml`. Any subdirectory nesting breaks detection entirely with no warning.

---

## 5. Delta OPTIMIZE Requires Storage Blob Data Owner (Not Contributor)

**Problem:** The streaming job wrote micro-batch files to silver successfully (requires `Storage Blob Data Contributor`). But OPTIMIZE + VACUUM, which physically deletes old files, requires `Storage Blob Data Owner` because it performs delete operations on files outside the standard write path.

**Solution:** Upgraded the Databricks managed identity RBAC role to `Storage Blob Data Owner` specifically on the silver and gold containers. Bronze remains `Contributor` (append-only).

---

## 6. Watermark State Growth Without Bounded Window

**Problem:** Early testing used a 60-minute watermark to cover SWIFT settlement delays. On a 4-node cluster processing 2.4M events/day, the in-memory state grew to 18GB within 6 hours, eventually causing OOM errors and cluster restarts.

**Solution:** Reduced watermark to 10 minutes — covering 99.2% of observed late arrivals from Event Hubs offset analysis. The remaining 0.8% are caught by the nightly Bronze reconciliation batch. State footprint reduced to ~800MB at steady state.

**Lesson:** Watermark duration is a memory vs. completeness tradeoff. Instrument your late arrival distribution before setting it. Never default to a large window without measuring the state growth impact.

---

## 7. Risk Score Always 0.0 for Round-Amount Transactions

**Problem:** The `score_large_round_amount` rule used Python modulo `% 1000`. PySpark's `%` operator on `DoubleType` is subject to floating-point precision errors — `10000.0 % 1000` returned `1.4210854715202004e-14` instead of `0.0`. The rule never fired.

**Solution:** Cast to `LongType` before modulo:

```python
((F.col("amount_gbp").cast("long") % 1000) == 0)
```

**Lesson:** Floating-point modulo in Spark is unreliable for financial amounts. Always cast to integer/long before modulo operations, or use `round()` to snap to nearest penny first.
