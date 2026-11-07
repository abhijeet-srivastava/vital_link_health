# Testing

Prefer a few high-value tests at the boundaries over blanket unit coverage. Integration tests run
against real PostgreSQL, Kafka and Redis with Testcontainers - constraint behaviour (`ON CONFLICT`,
partial unique indexes, `SKIP LOCKED`, `EXCLUDE`) cannot be mocked faithfully.

## Must-have tests (add or update when you touch the area)

| Area | Test |
| --- | --- |
| Ingest idempotency | Same batch sent concurrently from two threads → one row per reading, one outbox event, statuses `ACCEPTED` + `DUPLICATE` |
| Ingest integrity | Same `reading_id`, different values → `CONFLICT`, original row unchanged |
| Binding | Device bound to patient A posts for patient B → `403`; reading measured before a rebinding is stored for the old patient |
| Alert idempotency | `readings.ingested` delivered twice → one alert, `occurrence_cnt = 1`, one `alerts.raised` |
| Episodes | Two breaches same patient/measure → one alert, count 2; HIGH then CRITICAL → severity upgraded, one upgrade event |
| Batch fallback | Batch containing an already-evaluated reading → batch rolls back, per-event replay gives the same result as a clean run |
| Escalation | CRITICAL not acknowledged by `escalate_at` → escalated once with three scheduler instances running |
| Ack vs escalation race | Concurrent ack and escalation → exactly one wins; the other sees `409` or skips |
| Optimistic lock | Two acknowledges with the same version → one `200`, one `409` |
| Notifications | Same event consumed twice → one notification per coach and channel; second escalation after reopen → notifies again |
| Latest reading | Out-of-order readings never overwrite a newer cached value |
| Access control | Coach cannot read a patient outside their panel; denial is audited |
| AI fallback | LLM timeout → rule-based summary within the latency budget; ingest and alerts unaffected |
| Critical lane | CRITICAL alert notified while non-critical topic has a backlog |

## Conventions

- Test names state behaviour: `retried_batch_is_stored_once_and_announced_once`.
- One Testcontainers environment per test class (reuse containers); truncate between tests.
- Concurrency tests use a `CountDownLatch` start gate and real threads; assert on DB state, not
  logs.
- Time is injected (`Clock`); never `Thread.sleep` to wait for timers - advance the clock and run
  the scheduler step directly.
- Kafka consumers are tested by publishing to the real topic and awaiting DB state with a bounded
  poll (Awaitility).

## Load test

`load/` holds k6 scripts: burst of batched readings with 10% retries, and a replay of the 6 AM curve
at a 20% breach rate. Report throughput, p50/p95/p99 latency, duplicate rate, consumer lag and
commits/s in the README, with a sentence on where the bottleneck is.
