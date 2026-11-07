# Idempotency and concurrency

Every hop is at-least-once: devices retry, the relay can re-publish, Kafka can redeliver, and
schedulers run on several pods. Correctness rests on database constraints, never on application
locks, so each repeat is a no-op. Keep all eight guards intact in any change.

## The eight guards

| # | Where | Key or check | A repeat results in |
| --- | --- | --- | --- |
| 1 | Ingest insert | Reading PK `(device_id, reading_uuid, measured_at)`; outbox row only for rows `RETURNING` reports as new | Stored once; no second `readings.ingested` |
| 2 | Outbox relay | Same UUIDv7 `event_id` on re-publish; idempotent producer | Same event id downstream, absorbed by guard 4 |
| 3 | Evaluator, episode | Per-patient Kafka key; `FOR UPDATE` on the episode; unique active episode `(patient_id, measure_code)` | Folded into the existing alert, never a second alert |
| 4 | Evaluator, reading | `alert_occurrence` PK `(device_id, reading_uuid, measure_code, measured_at)` | Transaction rolls back: no count change, no new event |
| 5 | Notification record | `UNIQUE (trigger_event_id, coach_id, channel)` | No second row for the same change |
| 6 | External send | `PENDING → SENT`; provider idempotency key = `notification_id` | No second push or SMS |
| 7 | Escalation | `SKIP LOCKED` claim + `status = 'OPEN'` guard; `escalate_at` cleared in the same update; `CHECK` keeps the timer only on OPEN + CRITICAL | Escalated once |
| 8 | Coach action | `version` match; acknowledge clears `escalate_at` | Stale click gets `409`; acknowledged alert cannot escalate |

## Patterns (copy these shapes)

### 1. Ingest: insert, then announce only what is new

```sql
BEGIN;
INSERT INTO reading (device_id, reading_uuid, measured_at, patient_id, reading_type, device_seq,
                     systolic, diastolic, pulse, value_num, payload_hash)
SELECT * FROM unnest($1::bigint[], $2::uuid[], $3::timestamptz[], $4::bigint[], $5::smallint[],
                     $6::bigint[], $7::smallint[], $8::smallint[], $9::smallint[], $10::numeric[], $11::bigint[])
ON CONFLICT (device_id, reading_uuid, measured_at) DO NOTHING
RETURNING reading_uuid;
-- rows not returned: SELECT payload_hash for them; equal -> DUPLICATE, different -> CONFLICT (log integrity event)
INSERT INTO outbox_event (event_id, topic, event_type, aggregate_key, payload)
VALUES ($uuidv7, 'readings.ingested', 'readings.ingested', $patient_id, $new_readings_json);  -- skip if none new
COMMIT;
-- after commit, best effort: Redis Lua CAS on latest:{patient}:{type}
```

### 2. Evaluator: per event (fallback path and the clearest statement of the rules)

```sql
BEGIN;
SELECT 1 FROM alert_occurrence
WHERE device_id=$d AND reading_uuid=$r AND measure_code=$m AND measured_at=$t;   -- early exit if found
SELECT alert_id, status, severity FROM alert
WHERE patient_id=$p AND measure_code=$m AND status <> 'RESOLVED'
FOR UPDATE;
-- none: INSERT INTO alert (..., escalate_at = CASE WHEN CRITICAL THEN now() + interval '15 minutes' END)
-- found: UPDATE alert SET occurrence_cnt = occurrence_cnt + 1, last_seen_at = GREATEST(...),
--        severity = higher, status = (ACKNOWLEDGED -> OPEN if upgraded to CRITICAL),
--        escalate_at = (set if upgraded to CRITICAL), version = version + 1 WHERE alert_id = $id
INSERT INTO alert_occurrence (...) VALUES (...);          -- PK conflict -> ROLLBACK everything
INSERT INTO outbox_event (...) VALUES (...);              -- only new episode or severity upgrade
COMMIT;                                                   -- then commit the Kafka offset
```

### 3. Evaluator: batched (production path)

See `templates/java/BatchAlertEvaluator.java` and `references/performance-and-scaling.md`. Same
guards, one transaction per batch (≤ 500 events or 200 ms). Any unique violation rolls the batch
back and it is replayed per event with pattern 2. Correctness never depends on the batch path.

### 4. Coach state change

```sql
UPDATE alert
SET status = 'ACKNOWLEDGED', acknowledged_by = $coach, acknowledged_at = now(),
    escalate_at = NULL, version = version + 1
WHERE alert_id = $id AND version = $expected AND status IN ('OPEN','ESCALATED')
RETURNING *;
-- 0 rows -> 409 + current state. Same transaction: audit_log row + 'alert.state_changed' outbox row.
```

### 5. Escalation scheduler (safe on many pods)

```sql
UPDATE alert SET status = 'ESCALATED', escalate_at = NULL, version = version + 1
WHERE alert_id IN (SELECT alert_id FROM alert WHERE escalate_at <= now()
                   ORDER BY escalate_at LIMIT 100 FOR UPDATE SKIP LOCKED)
  AND status = 'OPEN'
RETURNING alert_id;   -- one 'alert.state_changed' outbox row per alert, same transaction
```

### 6. Notification record and send

```sql
INSERT INTO notification (trigger_event_id, alert_id, coach_id, channel, reason, severity)
VALUES ($event_id, $alert_id, $coach, $channel, $reason, $severity)
ON CONFLICT (trigger_event_id, coach_id, channel) DO NOTHING;

-- sender, per channel
SELECT * FROM notification
WHERE status = 'PENDING' AND channel = $ch AND next_attempt_at <= now()
ORDER BY next_attempt_at LIMIT 100 FOR UPDATE SKIP LOCKED;
-- call provider with idempotency key = notification_id; then status = 'SENT', sent_at = now()
-- on failure: attempts + 1, next_attempt_at = now() + backoff(attempts); after max -> 'FAILED' + page ops
```

### 7. Outbox relay (local poller; production uses Debezium)

```sql
SELECT * FROM outbox_event ORDER BY created_at LIMIT 500 FOR UPDATE SKIP LOCKED;
-- publish each with key = aggregate_key, header event_id; on broker ack: DELETE those rows; commit
```

Never use "everything after the last event_id I published": UUIDv7 and sequence values are not in
commit order, so a late-committing transaction would be skipped.

## Race conditions and their answers

| Scenario | Mechanism |
| --- | --- |
| Same batch retried, hitting two ingest pods at once | PK conflict; `RETURNING` tells each pod which rows it inserted |
| Retry reuses a reading id with different values | `payload_hash` mismatch → `CONFLICT` item status + integrity event |
| Two breaches for one patient/measure close together | Per-patient Kafka key; unique active episode as backstop |
| HIGH then CRITICAL in one episode | Severity upgraded in place; `SEVERITY_UPGRADED` notification |
| Kafka redelivers an evaluated event | Occurrence PK conflict rolls back |
| Out-of-order arrival | Redis CAS on `measured_at`; reads order by `measured_at` |
| Device rebound while readings are buffered | Binding resolved at `measured_at`; `EXCLUDE` prevents overlaps |
| Two coaches acknowledge together | Optimistic `version`; loser gets `409` |
| Coach acknowledges while scheduler escalates | Whichever commits first wins; the other sees changed status/version |
| Threshold changed mid-evaluation | Rules immutable; occurrence records exact `threshold_id`; ≤ 10 min cache staleness |
| Several relays polling | CDC in prod; `SKIP LOCKED` locally; consumers dedupe |
| Kafka rebalance overlaps two consumers | Guards 3 and 4; batch falls back to per-event path |
| Cache miss storm at 6 AM | Caffeine single-flight loading; jittered TTLs |
| Device clock wrong | Reject `measured_at` > now + 5 min or older than 90 days; DEFAULT partition alarm |

## Backpressure

- Ingest pods cap in-flight DB work with a semaphore; DB pool wait > 200 ms or outbox backlog over
  limit → `503` + jittered `Retry-After`. Devices buffer, so shedding is safe.
- Per-device token bucket in Redis → `429`.
- Consumers pause partitions when downstream writes slow; never buffer unbounded work in memory.
- Do not scale consumers up while the primary is saturated: more consumers mean more writes.
