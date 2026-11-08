# Decisions, assumptions and open questions

Append a dated line when a decision changes; never rewrite history. Full reasoning lives in the
design doc "HBox Platform - Architecture & Implementation Plan".

## Decisions

| # | Decision | Why |
| --- | --- | --- |
| D1 | Ingest commits and returns; alerting runs behind Kafka | Morning ingest must not stall on alert work |
| D2 | Transactional outbox for every event; Debezium in production | No lost events, no events without data |
| D3 | DB constraints are the idempotency mechanism | Survives retries, redelivery, rebalances and multiple pods |
| D4 | Device-generated `reading_uuid`; server derives UUIDv5 when absent | Only the device knows two requests are the same measurement |
| D5 | One reading row with typed columns (systolic, diastolic, pulse, value_num) | One measurement is one event; ~3× smaller than row-per-value at 50M/day |
| D6 | Daily partitions on `measured_at`; partition key in every unique key | Postgres requirement; time-bounded reads; cheap retention |
| D7 | Thresholds: one immutable band per row; severity = most severe band crossed | Explainable alerts; no conflicting rules |
| D8 | Alert = episode per (patient, measure); occurrences record each breaching reading | Prevents alert floods; occurrence PK is the alert idempotency key |
| D9 | Notifications keyed by `(trigger_event_id, coach_id, channel)` | Each real change notifies once; replays never do |
| D10 | Escalation from `escalate_at` (OPEN + CRITICAL), not `created_at` | Upgraded alerts get a full SLA window |
| D11 | Batched, set-based evaluation with per-event fallback | ~3,000 → ~300-600 commits/s at peak without weakening guards |
| D12 | `alert` fillfactor 80; repeat-breach columns unindexed | HOT updates, no index churn |
| D13 | Critical lane topic and notifier group | CRITICAL never waits behind lower severities |
| D14 | Kafka key `patient_id`, 128 partitions set up front | Per-patient order; partition growth would remap keys |
| D15 | Separate alerting DB, then shard both by `patient_id` | All hot queries are patient-scoped |
| D16 | Keep `synchronous_commit` on | Clinical data; throughput comes from batching |
| D17 | Explicit SQL on write paths (JdbcClient/jOOQ), no JPA there | `ON CONFLICT`, `RETURNING`, `unnest`, `SKIP LOCKED` must be visible |
| D18 | AI is advisory only, off the critical path | LLM failure can never block ingest or alerts |
| D19 | UUIDv7 `event_id` from the application | Unique across shards; time-ordered; no DB round trip |
| D20 | API keys as `key_id` + HMAC secret | Lookup by id; fast verification; rotation with two active keys |
| D21 | 2026-11-08: coach queue keyset order is `(severity_rank, created_at, alert_id)`; `ix_alert_queue` ends with `alert_id` | `created_at` is not unique; without a unique tie-breaker a cursor page can skip or repeat alerts created in the same microsecond |
| D22 | 2026-11-08: `reading_type_measure` maps reading types to measures; the evaluator splits readings with it, not `measure.reading_type` | PULSE comes from BP cuffs and pulse oximeters; one type per measure would drop pulse-ox pulse readings |
| D23 | 2026-11-08: ingest rejections - a batch with at least one valid reading returns `202` with per-item `REJECTED` + `code`; a batch where every reading is rejected returns `422` problem+json whose `code` is the shared rejection code (e.g. `READING_TOO_OLD`, `FUTURE_TIMESTAMP`) or `READINGS_REJECTED`, with per-item `results` | Reconciles api-contracts.md (per-item status) with "rejected with `422 READING_TOO_OLD`": with nothing to accept there is no "rest" to answer `202` for, and a 4xx tells the device not to retry |
| D24 | 2026-11-08: device bindings are read from the DB on every ingest request (one PK range query per batch); only credential rows are cached (Caffeine, 5 min, negatives included) | A rebinding must count at once: a reading attributed to the wrong patient is worse than one indexed query. Credential revocation takes up to 5 min until the Redis `dev:{keyId}` eviction tier lands |
| D25 | 2026-11-08: a breach for a patient with no current PRIMARY coach fails the event: bounded retries, then `readings.ingested.dlq` (metric `vitallink.evaluator.dlq`) | `alert.coach_id` is NOT NULL and an alert nobody owns would never be seen; retries cover a lagging care-team projection, the DLQ keeps the breach for replay instead of dropping it |
| D26 | 2026-11-08: evaluator folds keep `first_seen_at = LEAST(...)`, `last_seen_at = GREATEST(...)`, `is_late = is_late AND new breach is late`; the escalation window is one setting (`vitallink.alerting.escalation-window`, 15 min) until per-program windows land | Out-of-order breaches must not violate `last_seen_at >= first_seen_at`; one fresh breach makes the episode current again; per-program windows need a program attribute the alerting DB does not hold yet |
| D27 | 2026-11-08: the evaluator writes one `audit_log` row per alert state change in the evaluation transaction: `RAISE_ALERT`, `UPGRADE_ALERT`, `REOPEN_ALERT`; actor `SERVICE`/`alert-evaluator`, resource `alert/{id}`, request id = the `readings.ingested` `event_id`. A repeat breach (counters only) is not audited | SKILL.md rule 9 ("every alert state change writes `audit_log`") is stricter than the coach-only audit column in alert-processing.md; the stricter rule wins |
| D28 | 2026-11-08: evaluator retries are non-blocking: a failing record moves to `readings.ingested.retry` (fixed delay, 3 attempts), then `readings.ingested.dlq`; malformed records go to the DLQ at once | A blocking retry held the whole partition for the backoff, delaying CRITICAL readings of other patients. Cost: a retried event leaves its patient's order; folding is order-independent (LEAST/GREATEST, severity only rises), only RAISED vs SEVERITY_UPGRADED labelling can differ |

## Assumptions

1. A device belongs to one patient at a time; history kept in `device_binding`.
2. Each patient has one primary coach and optionally a backup.
3. Readings older than 7 days alert with `is_late`; older than 90 days are rejected.
4. Up to 20% of readings may breach a threshold (planning assumption for capacity).
5. Retention: 90 days hot, archive per legal policy (assumed 7 years).
6. Patient demographics live in another service; this service holds IDs only.

## Open clinical decisions (implement behind configuration)

| Question | Current default |
| --- | --- |
| Acknowledged alert rising to CRITICAL returns to OPEN and restarts escalation? | Yes |
| Reject readings older than 90 days? | Yes, `422 READING_TOO_OLD` |
| Multiple programs, no override: most conservative default? | Yes |
| Confirmation rule for non-critical alerts | Off unless enabled per program/patient |
| Daily digest for chronically out-of-range patients | Off unless set by a clinician |
| Escalation window | 15 minutes, configurable per program |
| 2026-11-08: Same PULSE thresholds whether the pulse came from a BP cuff or a pulse oximeter? | Yes, one set of PULSE bands per patient/program |
| 2026-11-08: Seed threshold bands need clinical sign-off before any real use | Seed rows carry `created_by = 'SEED-NOT-CLINICAL'`; apps refuse to start with the `prod` profile while one exists |
