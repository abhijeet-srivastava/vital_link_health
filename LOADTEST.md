# Load test: Readings & Alerting (stage 11)

All numbers come from runs on 2026-11-09 against `docker compose` on one laptop. Service configuration was the
committed one throughout; nothing was tuned between runs.

The first round of runs found a virtual-thread pinning deadlock in ingest-api. It was fixed (see
[The deadlock](#the-deadlock-found-and-fixed)) and every scenario was run again. Raw output: `load/results/`
(after the fix) and `load/results/before-fix/`.

## Machine and stack

| | |
| --- | --- |
| Host | Apple M4 (10 cores), 16 GB RAM, macOS 26.5.1 |
| Docker | Docker Desktop, engine 29.7.2; Linux VM with 10 CPUs and 7.75 GiB RAM shared by **every** container, k6 included |
| PostgreSQL | 16.15 + pg_partman, stock config (`shared_buffers` 128 MB, `max_wal_size` 1 GB, `synchronous_commit` on), Docker volume |
| Kafka | 3.9.2, one broker (KRaft), `readings.ingested` with 128 partitions |
| Services | one replica each. ingest-api: Hikari pool 10, `max-in-flight` 64, per-device bucket 30 / 60 per min, credential cache TTL 5 min. alert-worker: evaluator with 4 listeners, ≤ 500 events or 200 ms per transaction; local `SKIP LOCKED` relay |
| Load generator | k6 1.3.0 (`grafana/k6:1.3.0`) on the compose network |

## How to run

```
docker compose up -d
load/run.sh burst      # or: load/run.sh morning;  RUN_LABEL=name names the result files
```

`run.sh` takes the following steps:

1. **Device keys.** The seeded devices (1–1000, minus the five demo devices) get keys through the seed service's `LoadTestKeyExporter`. The keys go to `load/.keys/keys.csv`, which is gitignored and owner-only, and never to a log.
2. **Alert reset.** Unresolved alerts on load patients are resolved, so CRITICAL readings open new episodes.
3. **Samplers.** Evaluator lag and outbox backlog are sampled about every 2 s, along with `docker stats`.
4. **The run.** k6 runs, then the script waits for the outbox to empty and the evaluator lag to reach 0.
5. **Report.** `load/report.sql` and `load/check-duplicates.sql` run against the database.

What each script sends:

- **`load/burst.js`**: 200 devices, each sending 22 requests of 50 readings: about 20 new batches and about 10% deliberate resends of an earlier batch. On a 429 or 503 the device waits for `Retry-After` and sends the same body again. 5% of readings breach a threshold.
- **`load/morning.js`**: arrival rate shaped like 6–9 AM, compressed 20× into 9 minutes. It rises from 10% to a peak of 560 req/s × 5 readings, which is 2,800 readings/s (the capacity model's "morning sustained" figure), then tails off. Each request comes from a random one of 995 devices. 20% of readings breach a threshold, and 10% of those are CRITICAL. The 9 minutes cross the 5-minute credential TTL at the 0.85 plateau.
- **Thresholds.** k6 thresholds encode the targets, so a miss fails the run: ingest p99 < 150 ms, errors < 1%, zero ACCEPTED readings in a retried batch, and zero dropped iterations.

## Results (after the fix)

| | Burst, cold 1 | Burst, cold 2 | Burst, warm | Morning curve |
| --- | --- | --- | --- | --- |
| Readings stored / sent | 199,300 / 199,300 | 198,100 / 198,100 | 198,600 / 198,600 | 910,505 / 910,505 |
| Throughput: average over the run / peak 1 s | 6,258 / 24,450 rows/s | 6,210 / 35,900 | 6,297 / 47,900 | 1,681 / 2,995; plateau 2,823 |
| Ingest latency of new batches, p50 / p95 / p99 | 8.9 / 266 / 631 ms | 9.7 / 156 / 539 ms | 8.2 / 91 / 162 ms | 1.5 / 9.9 / 106 ms |
| Failed requests (500 or timeout) | 0 | 0 | 0 | 0 |
| Requests shed (429/503), then retried | 136 of 4,400 | 136 of 4,400 | 141 of 4,400 | 2,415 of ~184,500 (1.3%) |
| Duplicate rate: extra rows from retries | **0** (20,700 DUPLICATE) | **0** (21,900) | **0** (21,400) | **0**; `check-duplicates.sql` = 0 on the whole DB |
| Max evaluator consumer lag | 35 records | 50 | 16 | 115 |
| Max outbox backlog | 56 rows, 0.2 s old | 81, 0.2 s | 197, 0.2 s | 84, 0.4 s |
| Pipeline drain after k6 ends | 1.6 s | 1.5 s | 1.4 s | 1.2 s |
| Evaluator commits/s: average (peak 1 s) | 15.3 (46) | 15.2 (68) | 14.4 (68) | 19.6 (20) |
| Occurrences per evaluator commit: average / max | 21.2 / 166 | 21.5 / 80 | 22.8 / 217 | 17.2 / 68 |
| All database commits/s | 322 | 315 | 316 | 721 |
| CRITICAL reading → alert in coach queue, p50 / p95 / p99 | 106 / 262 / 328 ms | 101 / 225 / 284 ms | 106 / 250 / 354 ms | 161 / 271 / 302 ms (18,232 readings) |
| CRITICAL reading → QUEUE notification, p50 / p95 / p99 | 236 / 391 / 482 ms | 223 / 362 / 422 ms | 228 / 386 / 439 ms | 215 / 312 / 415 ms (995 episodes) |
| Peak container CPU, % of one core: ingest-api / Postgres / alert-worker | — | — | 282 / 245 / 39 | 169 / 212 / 79 |
| k6 thresholds | failed: p99 631 ms | failed: p99 539 ms | failed: p99 162 ms | failed: 179 dropped iterations (0.1%) |

Notes on the morning column:
- **Dropped iterations:** k6 dropped 179 iterations when all 1,000 VUs were busy. Most of those VUs were sleeping out a 5–30 s `Retry-After` after a shed request.
- **Shed split:** the 429/503 split of the shed requests was not recorded. A 429 is unlikely: at about 0.56 req/s per device, the per-device bucket of 30 is never exhausted. So these are probably 503s from the in-flight cap. The scripts now tag the status, so the next run will show the split.

**How these were measured.** There is no metrics stack in this cut (stage 9 was dropped), so pipeline numbers come from database timestamps:

- **Timestamps.** `reading.received_at` and `alert_occurrence.created_at` are transaction start times. "Alert in coach queue" is therefore measured to the start of the evaluator transaction, which writes the alert row the queue reads. That excludes the transaction's own commit, which is bounded by the 200 ms batch window.
- **Evaluator commits.** Counted as distinct `alert_occurrence.created_at` values, so they cover only transactions that wrote at least one breach.
- **Lag and backlog.** Sampled every 1–2 s, so short peaks can be missed. The lag sampler runs `kafka-consumer-groups.sh` inside the Kafka container, which inflates that container's CPU (up to 327%), so Kafka CPU is left out of the table.

## The deadlock (found and fixed)

**Before the fix** (`load/results/before-fix/`):

| | Burst run 1 (cold) | Burst run 2 (cold) | Burst run 4 (warm) | Morning curve |
| --- | --- | --- | --- | --- |
| Readings stored / sent | 154,000 / 203,050 | 199,800 / 199,800 | 199,250 / 199,250 | 609,905 / 649,665 |
| Failed requests | **1,000 (23%)** | 0 | 0 | **7,981 (6.1%)**; 52,346 iterations dropped |
| What happened | almost no writes for 2.5 min, broken only by 30 s pulses | — | — | at t0 + 5:02 ingest fell from about 2,450 rows/s to about 5 rows/min and never recovered; restart needed |
| Ingest latency of new batches, p50 / p95 / p99 | 5.4 / 21 / 42 ms (on the requests that succeeded) | 9.7 / 137 / 525 ms | 7.2 / 101 / 161 ms | 1.6 / 17.6 / 103 ms (whole run, successful requests only) |
| Duplicate rows | 0 | 0 | 0 | 0 |

Run 3, a warm run like run 4 but without the alert reset, is also in `load/results/before-fix/`.

**The cause.** `CachingDeviceDirectory` loaded a missing credential with Caffeine's `get(key, loader)`. That runs the JDBC lookup inside `ConcurrentHashMap.compute`, which holds a `synchronized` bin lock. On JDK 21 a virtual thread that parks for a Hikari connection while holding that lock pins its carrier.

**When it fires.** Once 10 requests (the number of carriers) are waiting on cache misses at the same moment, every carrier is blocked. The requests that already hold the 10 pool connections can then never be scheduled to finish and release them.
- On a cold start this happens only sometimes; it depends on timing.
- In the morning run it happened at TTL expiry: about 1,000 keys loaded during the first minute all expired together, five minutes later, at peak load.
- `-Djdk.tracePinnedThreads` showed the `ConcurrentHashMap.compute` frame. A thread dump of the wedged process showed all 10 carriers mounted while the process sat idle.

**The fix (ingest-api).**
- `CachingDeviceDirectory` is now a Caffeine `AsyncCache`. The map compute only installs a future, and the lookup runs on a dedicated virtual-thread executor (`credentialLoader` in `IngestConfig`).
- The request waits in `join()` outside any monitor, so it doesn't pin. Concurrent misses for the same key still share one load.
- A failed load is removed at once, so the next request retries it.
- Expiry is jittered across the last 10% of the TTL (the TTL is also the revocation latency, so it is never exceeded).
- Regression test: `CachingDeviceDirectoryTest`.
  - `cache_misses_beyond_the_carrier_count_do_not_pin_carriers` reproduced the hang on the old code (timed out at 10 s) and passes on the new code.
  - The other tests cover single-flight loading, cached unknown keys, no caching of failures, and the expiry jitter.

**Checked during the reruns.** With `-Djdk.tracePinnedThreads=short` on, there were 0 pins in `ConcurrentHashMap.compute`. The only pin traces were Hikari's one-time pool initialisation at each of the 4 startups. There were 0 `CannotGetJdbcConnectionException`s, and ingest-api stayed healthy with no restart.

## Where the bottleneck is now

**With the deadlock gone, the limit is ingest-api's per-pod admission and latency.** Postgres, Kafka and the evaluator have headroom.

- **Database.** It absorbs a burst at 25–48k rows/s for about 2 s, then sits mostly idle (Postgres peaked at about 2.5 of 10 cores).
- **Admission.** Each pod admits only 64 requests at once over a 10-connection pool. About 3% of burst requests (136–141 of 4,400) get a 503, and their 5–30 s `Retry-After` stretches a 2-second burst into a 31-second tail.
- **Latency targets.** Cold p99 is 540–630 ms and warm p99 is 162 ms for 50-reading batches. Both miss the 150 ms target, which is stated for 100-reading batches. The cold excess is the first credential loads and JIT warm-up.
- **Morning curve.** It met the 2,800 readings/s target: the plateau held 2,823 rows/s with p99 106 ms, 0 errors, 1.3% shed and the critical path at p95 271 ms (SLO 5 s).
- **Downstream.** The pipeline never lagged more than 115 records or 0.4 s, and evaluator commits/s stayed at about 20 with about 20 occurrences per commit.

**What would move it, in order:**
1. More ingest pods, or a larger pool behind PgBouncer, so one pod's 10 connections and 64-request cap stop deciding admission.
2. A shorter, load-aware `Retry-After`, so shed devices come back while capacity is free.
3. Pre-warming the credential cache on start (or the planned Redis `dev:{keyId}` tier) for cold p99.

The next ceiling named in `performance-and-scaling.md` (primary WAL/IOPS, then evaluator commit rate) was not reached on this machine.

**Follow-ups for stage 12:**
- Move to JDK 24+/25 (JEP 491 removes monitor pinning everywhere).
