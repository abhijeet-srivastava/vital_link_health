# Architecture

The ingest request ends at a PostgreSQL commit; everything about alerts happens afterwards, driven
by Kafka. That one boundary removes the monolith's morning stall and lets ingest, alerting and reads
scale separately.

## Deployables (one codebase, three scaling profiles)

| Deployable | Responsibility | Scales on |
| --- | --- | --- |
| ingest-api | Validate batch, device auth, binding check at `measured_at`, dedup insert, outbox row, latest-reading cache update after commit | CPU + DB write IOPS; HPA on CPU and in-flight requests; pre-scaled from 5:30 AM |
| alert-worker | Threshold evaluator (batched), notifier, escalation scheduler, AI triage consumer - separate consumer groups | Kafka partitions (max pods = partitions); KEDA on consumer lag |
| query-api | Latest readings, range queries, coach alert queue, AI summary | Read replicas + Redis |

Supporting: API gateway (TLS, device key / OIDC, per-device rate limits, size caps, WAF), outbox
relay (Debezium CDC in production; `FOR UPDATE SKIP LOCKED` poller locally), Redis, PostgreSQL.

## Request flows

### Ingest (write path)

1. Device posts 1-500 readings; gateway authenticates the key and rate-limits.
2. ingest-api resolves `key_id → device_id`, checks the binding at each reading's `measured_at`,
   validates plausibility ranges from `measure`, rejects `measured_at > now + 5 min` and older than
   90 days.
3. One transaction: `INSERT … ON CONFLICT DO NOTHING RETURNING` readings; conflicting rows compared
   by `payload_hash` (`DUPLICATE` vs `CONFLICT`); one `readings.ingested` outbox row for the new
   rows only.
4. Commit, return `202` with per-item status. Then best-effort Redis compare-and-set of the latest
   reading.

### Alerting

1. Relay publishes `readings.ingested` keyed by `patient_id`.
2. Evaluator consumes batches (≤ 500 events or 200 ms), splits readings into measures, evaluates
   threshold bands, writes episodes + occurrences + outbox set-based in one transaction.
3. Relay publishes `alerts.raised` or `alerts.raised.critical` for new episodes and upgrades.
4. Notifier writes one `notification` per change, coach and channel; senders deliver PUSH/SMS.
5. Escalation scheduler escalates CRITICAL alerts still `OPEN` at `escalate_at`.

### Reads

query-api reads Redis for latest values, replicas for ranges and the coach queue, keyset
pagination only, always scoped by patient (and coach panel for coaches).

## Kafka topics

| Topic | Key | Partitions | Producer | Consumers |
| --- | --- | --- | --- | --- |
| `readings.ingested` | `patient_id` | 128 | ingest outbox | evaluator |
| `alerts.raised` | `patient_id` | 32 | alerting outbox | notifier, AI triage |
| `alerts.raised.critical` | `patient_id` | 16 | alerting outbox | critical notifier group |
| `alert.state_changed` | `patient_id` | 32 | alerting outbox | notifier (escalation, reassignment), audit export |
| `thresholds.changed` | `patient_id` | 8 | threshold API outbox | all pods (cache eviction) |
| `device.binding_changed` | `device_id` | 8 | device admin outbox | ingest pods (cache eviction) |
| `<topic>.dlq` | same | same | consumers after N retries | ops tooling |

Partition counts are set up front: adding partitions later remaps keys and breaks per-patient
ordering during the change. Event payloads carry IDs and values only and a `schema_version`; the
`event_id` (UUIDv7) also travels as a Kafka header.

### Example payloads

```json
{ "event_id": "0192f3c4-7a1e-7b2c-9d10-5e6f7a8b9c0d", "type": "readings.ingested", "schema_version": 1,
  "patient_id": 8812, "device_id": 50311,
  "readings": [ { "reading_uuid": "7c1e...", "reading_type": "BLOOD_PRESSURE", "device_seq": 10422,
                  "measured_at": "2026-11-07T06:42:10Z", "systolic": 186, "diastolic": 94, "pulse": 78 } ] }
// single-value types (glucose mg/dL, SpO2 %, weight kg) carry "value" in the canonical unit; null fields are omitted

{ "event_id": "...", "type": "alerts.raised", "schema_version": 1,
  "alert_id": 99102, "patient_id": 8812, "coach_id": 41, "measure": "SYSTOLIC",
  "observed_value": 186, "severity": "CRITICAL", "reason": "RAISED", "threshold_id": 7,
  "occurrence_cnt": 1, "is_late": false }

{ "event_id": "...", "type": "alert.state_changed", "alert_id": 99102,
  "from": "OPEN", "to": "ESCALATED", "actor": "scheduler", "alert_version": 3 }
```

## Caching (Redis + in-process Caffeine L1)

| Key | Holds | Write strategy | Invalidation |
| --- | --- | --- | --- |
| `latest:{patientId}:{readingType}` | Latest reading | After commit, Lua compare-and-set on `measured_at` | Never overwritten by older; TTL 7 days |
| `thr:{patientId}:{measureCode}` | Effective threshold bands | Cache-aside, TTL 10 min (jittered) | `DEL` on change + `thresholds.changed` evicts L1 |
| `dev:{keyId}` | Device id, current binding | Cache-aside, TTL 5 min | Evicted on rotation, revocation, rebinding |
| `rate:{deviceId}` | Per-device token bucket (tokens, last refill) | Lua script per ingest request, Redis clock | Expiry once full; on Redis loss, no rate limit (fail open) |
| `idem:{deviceId}:{readingUuid}` | Seen marker | `SET NX`, TTL 24 h | Expiry only; DB key is the guarantee |
| `confirm:{patientId}:{measureCode}` | Recent breach timestamps (confirmation rule) | Sorted set, trimmed to window | Expiry; on Redis loss, alert immediately |
| `summary:{patientId}` | AI summary | Cache-aside, TTL 15 min | Evicted on new alert |

Redis is never required for correctness: every miss or outage falls back to the database.

## Database placement

- Now: one primary; alerting tables in their own schema.
- When evaluator commits pass ~30% of primary capacity: separate **ingest DB** (devices, readings,
  ingest outbox, audit) and **alerting DB** (thresholds, projections, alerts, occurrences,
  notifications, alerting outbox, processed_event, audit). Each keeps its own outbox; no
  transaction spans both.
- At full scale: shard both by `patient_id` (device tables by `device_id`), e.g. Citus.

## Partial outages

| Down | Ingest | Alerts | Reads |
| --- | --- | --- | --- |
| Kafka | Works; outbox grows | Delayed, then drained in order | Unaffected |
| Redis | Works; auth from DB behind a circuit breaker | Thresholds from DB | Latest from replicas |
| Postgres primary | `503` + `Retry-After`; devices buffer | Paused | Replicas, stale |
| LLM provider | Unaffected | Unaffected; `ai_priority` empty | Rule-based summary fallback |
| One consumer pod | Unaffected | Cooperative-sticky rebalance; resumes from last commit | Unaffected |

## Production targets

Managed equivalents: RDS/Aurora PostgreSQL, MSK, ElastiCache, Kubernetes, KMS for keys and the
API-key pepper, S3 (Object Lock) for archives and audit exports.
