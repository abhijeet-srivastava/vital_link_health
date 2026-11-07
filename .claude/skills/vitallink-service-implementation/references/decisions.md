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
