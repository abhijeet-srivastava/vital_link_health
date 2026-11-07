# Performance and scaling

Size for the morning burst, not the daily average. All figures are planning estimates; confirm
them with the k6 load test before treating them as limits.

## Capacity model

| Quantity | Estimate | Basis |
| --- | --- | --- |
| Daily average | ~580 readings/s | 50M ÷ 86,400 |
| Morning sustained | ~2,800 readings/s | 60% of volume in 6-9 AM |
| Morning spike | ~15,000 readings/s | 5× sustained at reconnect bursts |
| Requests at spike | ~3,000 req/s | Average batch of 5 |
| Breaches at spike | ~3,000/s | Planning assumption: up to 20% of readings breach |
| Breaches per day | ~10M | ~1-1.5 GB/day of `alert_occurrence` |
| Hot storage | ~8 GB/day readings incl. indexes; 90 days ≈ 720 GB | ~80 B row + ~80 B index |

## Targets (SLOs)

- Ingest p99 < 150 ms for a 100-reading batch.
- Critical alert visible in the coach queue < 5 s p95 after ingest.
- Latest reading from cache; 30-day range query < 100 ms.

## Ingest path

- One multi-row `INSERT … ON CONFLICT DO NOTHING RETURNING` (via `unnest`) + one outbox insert per
  request, in one short transaction.
- Java 21 virtual threads; HikariCP ~20 connections per pod behind PgBouncer (transaction mode).
- Auth, validation, plausibility and rate limits before touching the DB.
- Redis latest-reading update after commit, best-effort.
- Stateless pods; HPA on CPU and in-flight requests; scheduled pre-scale from 5:30 AM.

## Alert pipeline

| At ~3,000 breaches/s | One transaction per breach | Batched (required design) |
| --- | --- | --- |
| Evaluator commits/s | ~3,000 | ~300-600 |
| Statements/s | ~13,000 | ~2,000-3,000 |
| Index writes per repeat breach | 3 | 0 (HOT update) |
| Load on ingest primary | Shared | None once alerting has its own primary |
| Critical alert latency | Grows with lag in spikes | Batch commit ≤ 200 ms; critical lane |

Rules:

1. **Set-based batches**: ≤ 500 events or 200 ms, one transaction; dedup query, one
   `FOR UPDATE` over all touched episodes, bulk insert/update via `unnest`, bulk occurrence and
   outbox inserts. Merge several breaches for the same patient/measure in memory first.
2. **Fallback**: on any unique violation, roll back and replay the batch per event.
3. **HOT updates**: `alert` has `fillfactor = 80`; repeat breaches change only unindexed columns.
   Do not add an index on `occurrence_cnt`, `last_seen_at` or `version`.
4. **Partitioned occurrences**: `alert_occurrence` daily partitions, 90-day retention.
5. **Critical lane**: outbox `topic = alerts.raised.critical` for CRITICAL; dedicated notifier
   pods; reserved provider quota.
6. **Kafka**: 128 partitions on `readings.ingested`; KEDA on lag; max pods = partitions;
   cooperative-sticky assignor.
7. **Thresholds**: Caffeine L1 → Redis `MGET` per batch → one DB query for misses.
8. **Durability**: `synchronous_commit` stays on. Batch instead.
9. **Provider throughput**: token bucket per provider; `PENDING` rows drain at provider rate.

## Data at scale

- Daily range partitions on `measured_at` (pg_partman, 7 days ahead); DEFAULT partition alarmed.
- Retention: 90 days hot → detach → Parquet on S3 (encrypted, Object Lock) → drop; Glacier after
  1 year; total retention per legal policy (assumed 7 years).
- Rollup table (hourly/daily min, max, avg, count per patient per measure) for trends and AI
  summaries; never scan raw rows for long ranges.
- Read replicas for query-api; keyset pagination only.

## Database growth path

1. One primary; alerting tables in their own schema.
2. When evaluator commits pass ~30% of primary capacity → separate alerting primary.
3. At full target → shard ingest and alerting DBs by `patient_id`; batches split per shard.

## Alert volume a coach can act on

A 20% breach rate is ~10M breaches/day; even 5% becoming episodes would swamp coaches. Built-in
controls, each enabled by a clinician: confirmation rule (never for CRITICAL), per-patient
baselines as patient overrides, daily digest via `notification_policy`, and a
new-episodes-per-coach-per-day metric with an alert above agreed capacity.

## What to measure

| Signal | Alert when |
| --- | --- |
| Ingest p50/p99 per stage (auth, insert, outbox) | p99 > 300 ms for 5 min |
| DB pool wait, insert latency, WAL rate, replica lag | Pool wait > 100 ms |
| Consumer lag per partition; freshness | Lag > 60 s; critical lane > 5 s |
| Evaluator batch size, commit latency, rollbacks to per-event path | Commit p99 > 200 ms; rollbacks outside a rebalance |
| Duplicate ratio, CONFLICT count, DEFAULT partition rows, DLQ depth | Any DLQ growth; DEFAULT rows > 0 |
| Critical alert ingest → queue | p95 > 5 s |
| New episodes per coach per day; provider queue depth | Above agreed capacity |
| LLM latency, error and fallback rate, daily cost | Fallback rate > 20% |

## Load test

k6: 200+ virtual devices, 50-reading batches, 10% deliberate retries, replay of the 6 AM curve at
a 20% breach rate. Record throughput, p50/p95/p99, duplicate rate, consumer lag, commits/s, and
where the bottleneck moves (Postgres WAL/IOPS → Kafka producer → evaluator commits).
