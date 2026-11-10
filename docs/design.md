# HBox Platform — Architecture & Implementation Plan

Nov 5, 2026 · Abhijeet · Source of truth: [live design doc](https://claude.ai/artifact/CBKPwbpYbhHwWhCgCTvBhi)

> Aligned with the `vitallink-service-implementation` skill on 2026-11-07. Where this document and the skill disagree, the skill (and `references/schema.sql` first) wins.

## Context and scope

We carve **Readings & Alerting** out of the VitalLink monolith as a standalone service that ingests device vitals fast, dedupes retries, persists to PostgreSQL, and raises alerts asynchronously through Kafka. The design targets \~1M active devices and \~50M readings/day, with most traffic between 6 and 9 AM.

**Problems we are fixing**

- Ingest stalls at the morning peak because alert evaluation runs synchronously in the request path.
- No separation between intake, storage, alerting and read traffic, so one slow part drags down the rest.
- Device retries create duplicate readings and duplicate alerts.

**Goals**

- Ingest p99 < 150 ms for a 100-reading batch; the request never waits on alert work.
- Exactly one stored reading and at most one alert per device retry (effectively-once).
- Critical alert visible in a coach's queue within 5 s of ingest at p95.
- Fast reads: latest reading per patient from cache; 30-day range queries < 100 ms.
- PHI-grade security: encryption, scoped access, full audit trail.

**Non-goals for the first cut**: every device type (BP cuff ships first), a UI, real SMS/push delivery, ML-based anomaly detection.

**Stack decision**: Java 21 + Spring Boot 3, PostgreSQL 16 (native partitioning), Kafka, Redis 7, Docker Compose locally. Production on managed equivalents (RDS/Aurora Postgres, MSK, ElastiCache) on Kubernetes.

**Assumptions we write down**

1. A device belongs to exactly one patient at a time; reassignment closes the old binding.
2. Every reading carries a device-generated `measured_at` and a device sequence number or reading UUID.
3. Thresholds are per patient per measure (one band per row), with program-level defaults.
4. Each patient has one primary coach; panel changes are rare compared to ingest.
5. A reading older than 7 days is accepted and stored, but alerts on it are marked `LATE` and routed at lower priority. Readings older than 90 days (the hot window) are rejected with 422 and counted.

## Architecture

The ingest request ends at a PostgreSQL commit; everything about alerts happens afterwards, driven by Kafka. That one boundary removes the monolith's morning stall and lets ingest, alerting and reads scale separately.

> Diagram: target-state architecture · write path left, read path right. See the [live design doc](https://claude.ai/artifact/CBKPwbpYbhHwWhCgCTvBhi) for the drawing.

The write path (left) commits a reading batch and its outbox row together, so no event is lost and no event exists without its data. The read path (right) never touches the primary for hot queries.

### Request flow

1. Device posts a batch; the gateway authenticates the API key and rate-limits.
2. ingest-api checks the device is bound to the patient, validates, and inserts readings with `ON CONFLICT DO NOTHING` plus outbox rows for new readings only, in one transaction. It returns `202` with per-reading status.
3. After commit, the latest-reading cache is updated (best-effort).
4. The relay publishes `readings.ingested`; the evaluator raises or coalesces alerts and emits `alerts.raised`.
5. The notification worker records one notification per change, coach and channel; the coach sees the alert in their queue via query-api, and PUSH/SMS senders deliver the rest.

### Security and PHI

- **Identity**: devices use per-device API keys (hashed, rotatable, mTLS as a later upgrade); coaches use OIDC with scopes such as `alerts:read`; services use OAuth2 client credentials.
- **Authorization**: a coach can only read patients on their panel. Checked in query-api against the panel table and enforced again with Postgres row-level security.
- **Encryption**: TLS 1.2+ everywhere, including Kafka and Redis; KMS-managed encryption at rest for Postgres, Kafka, Redis and S3.
- **Minimum necessary**: the service stores `patient_id`, not names or addresses. Kafka events and logs carry IDs only; log scrubbing blocks values tagged as PHI.
- **Audit**: every PHI read and every alert state change writes to the append-only `audit_log`, shipped to WORM storage.
- **AI**: prompts are de-identified, the provider is covered by a BAA, and outputs are labelled advisory.

### Observability

| Signal | What we watch | Alert when |
| --- | --- | --- |
| Ingest latency | p50/p99 per stage (auth, insert, outbox) via OpenTelemetry spans | p99 > 300 ms for 5 min |
| Freshness | `now() − measured_at` of the newest alert per partition; Kafka consumer lag | Lag > 60 s |
| Database | Pool wait time, insert latency, WAL rate, locks, replica lag, autovacuum | Pool wait > 100 ms |
| Correctness | Duplicate ratio, `DEFAULT` partition row count, DLQ depth | Any DLQ growth; DEFAULT rows > 0 |
| Business SLO | Critical alert ingest-to-queue time | p95 > 5 s |
| AI | LLM latency, error rate, fallback rate, cost per day | Fallback rate > 20% |
| Evaluator batches | Batch size, commit latency, breaches per commit, batch rollbacks to the per-event path | Commit p99 > 200 ms, or rollbacks outside a rebalance |
| Alert volume | Breach rate per measure; new episodes per coach per day; critical-lane lag; provider queue depth | Any coach above agreed capacity; critical lag > 5 s |

### Live incident: 7:15 AM, ingest p99 ×3, alert lag rising

Ingest latency and alert lag rising together point to a shared dependency, and the only one both paths write to is the Postgres primary. So the first look is the ingest trace breakdown: is the time in DB pool wait, in the insert itself, or in Redis or auth?

| Likely cause | How to tell | Stabilize now | Prevent |
| --- | --- | --- | --- |
| Postgres write saturation (IOPS, WAL, checkpoint) | Insert span dominates; high WAL rate, IO wait on primary | Shed with `503 Retry-After`; pause AI triage and other non-critical consumers; stop any batch job on primary | Pre-scaled IOPS before 6 AM; shard plan trigger at 70% |
| Missing day partition, rows piling into `DEFAULT` | `DEFAULT` partition count rising; lock waits on it | Create the partition (rows move after), pause relay briefly if needed | pg\_partman 7 days ahead + alarm on DEFAULT rows |
| Redis slow or evicting, auth falling back to DB | Auth span up, Redis latency/evictions up, DB read QPS spike | Raise local Caffeine TTL, scale Redis, circuit-break to cached identities | Memory headroom alarms; separate Redis for rate limits |
| Poison message or hot partition in evaluator | Lag on one or two partitions only; ingest normal | Route the message to DLQ, restart consumer | Bounded retries then DLQ; per-partition lag alerts |

What not to do: scale consumers up while the primary is saturated. More consumers mean more writes and make ingest worse. After the incident: a blameless review, a load test that replays the 6 AM curve, and runbook steps added for each cause above.

## High-level design

The service splits into three independently scaled deployables that share one codebase: **ingest-api** (write path), **alert-worker** (Kafka consumers), and **query-api** (read path). They talk only through PostgreSQL, Redis and Kafka topics, never by direct calls.

### Core entities

| Entity | Owns | Key relationships | Write rate |
| --- | --- | --- | --- |
| Patient | Identity reference only (PHI lives in the patient service; we hold `patient_id`) | Enrollments, device bindings, care assignments | Low |
| Program / Enrollment | RPM program and which patients are in it (local projection) | Supplies default threshold rules | Very low |
| CareCoach / CareAssignment | Primary and backup coach per patient (local projection) | Routes alerts; limits what a coach can read | Low |
| Measure | Reference list of measurable quantities and their units and plausible ranges | Used by readings, rules and alerts | Static |
| Device / DeviceCredential | Serial, type, status; API keys with a public key id and hashed secret | Bound to one patient at a time via DeviceBinding | Low |
| Reading | One measurement: values, `measured_at`, idempotency key | Belongs to device + patient at `measured_at` | **Very high (\~50M/day)** |
| ThresholdRule | One band per row: measure, `GT`/`LT`, limit, severity; immutable versions | Patient override or program default | Low |
| Alert | One episode per patient and measure while unresolved | Many AlertOccurrences; assigned coach | Medium |
| AlertOccurrence | Each breaching reading folded into an episode | One reading, one rule version | Medium |
| Notification | A record that a coach was told, and why | One alert, one coach, one reason | Medium |
| OutboxEvent | Event written in the same transaction as the data it announces | Five event types, keyed by patient or device | Very high, short-lived |
| AuditLog | Append-only record of every PHI access and state change | Actor, patient, request | High |

### Key components

| Component | Responsibility | Scales on |
| --- | --- | --- |
| API gateway | TLS, device auth (API key → device identity), rate limits per device, request size caps | Connections |
| ingest-api | Validate batch, check device↔patient binding, dedupe, bulk insert readings + outbox rows in one transaction, update latest-reading cache | CPU + DB write IOPS |
| Outbox relay | Polls/streams outbox (Debezium CDC in prod) and publishes `readings.ingested` to Kafka, keyed by `patient_id` | Kafka throughput |
| Threshold evaluator (alert-worker) | Consumes `readings.ingested`, loads thresholds (Redis → DB), creates Alert rows idempotently, emits `alerts.raised` | Kafka partitions |
| Notification worker (alert-worker) | Two consumer groups: `notifier` (`alerts.raised`) and `notifier-critical` (`alerts.raised.critical`, plus `alert.state_changed` in its own container). Resolves recipients from `care_assignment`, writes Notification rows (QUEUE rows `SENT` at once; the queue reads `alert`), and PUSH/SMS senders deliver `PENDING` rows | Kafka partitions; senders by provider rate |
| Triage worker (AI, optional) | Adds an advisory LLM priority to an alert; timeouts fall back to the rule severity | Rate-limited, off the critical path |
| query-api | Latest readings, time-range reads, coach alert queue; reads replicas + Redis | Read replicas |
| Redis | Latest reading per patient, threshold cache, idempotency fast-check, rate-limit counters | Memory |
| PostgreSQL | Source of truth; readings partitioned by day | Partitions, later shards |

### APIs

All endpoints are versioned under `/v1`. Devices authenticate with `X-Device-Key`; coaches and services use OAuth2/OIDC bearer tokens with scopes.

| Method & path | Caller | Purpose | Success |
| --- | --- | --- | --- |
| `POST /v1/devices/{deviceId}/readings` | Device | Ingest 1–500 readings in one batch | `202` with per-reading status: `ACCEPTED`, `DUPLICATE`, `CONFLICT` or `REJECTED` |
| `GET /v1/patients/{patientId}/readings/latest?type=` | Coach, service | Latest reading per reading type | `200` (from Redis, DB fallback) |
| `GET /v1/patients/{patientId}/readings?type=&from=&to=&cursor=&limit=` | Coach, service | Time-range query, keyset pagination (limit ≤ 500, span ≤ 90 days) | `200` + `next_cursor` |
| `GET /v1/coaches/{coachId}/alerts?status=OPEN&cursor=` | Coach | Alert queue, ordered by severity then age | `200` |
| `POST /v1/alerts/{alertId}/acknowledge` | Coach | Move alert to `ACKNOWLEDGED`; needs `If-Match` version | `200` or `409` |
| `POST /v1/alerts/{alertId}/resolve` | Coach | Close with a note | `200` or `409` |
| `GET /v1/patients/{patientId}/summary` | Coach | AI plain-language summary of recent readings, cached 15 min | `200`, or `200` with a rule-based fallback |
| `PUT /v1/patients/{patientId}/thresholds/{measure}` | Clinician, admin | Replace bands (closes old rows, inserts new); invalidates cache | `200` |
| `PUT /v1/patients/{patientId}/notification-policy` | Clinician | `IMMEDIATE` or `DAILY_DIGEST` for non-critical alerts | `200` |

**Ingest request (single reading is a batch of one)**

```json
{
  "patient_id": 8812,
  "readings": [
    {
      "reading_id": "7c1e0b7a-2f7e-4c1d-9a51-1d2b3c4d5e6f",
      "device_seq": 10422,
      "type": "BLOOD_PRESSURE",
      "measured_at": "2026-11-05T06:42:10Z",
      "values": {"systolic": 186, "diastolic": 94, "pulse": 78},
      "unit": "mmHg"
    }
  ]
}
```

`patient_id` is the numeric id (`BIGINT` in the schema). `type` is the reading type; the evaluator splits a reading into measures (`measure_code`: systolic, diastolic, pulse ...).

Per-item `status`: `ACCEPTED` (new), `DUPLICATE` (same id, same values), `CONFLICT` (same id, different values; logged as an integrity event), `REJECTED` (with a `code` such as `IMPLAUSIBLE_VALUE`, `READING_TOO_OLD`, `FUTURE_TIMESTAMP`). A batch with some rejected items still returns `202` for the rest.

**Errors** use RFC 7807 problem JSON: `401` bad key, `403` device not bound to that patient at `measured_at`, `413` batch over 500, `422` malformed body, `429` rate limited with `Retry-After`, `503` backpressure with `Retry-After`.

## Low-level design

Readings live in a day-partitioned PostgreSQL table whose unique key doubles as the idempotency guard; alerts and notifications carry their own natural unique keys, so every retry at every stage is a no-op insert.

### Schema: entities and fields

```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;   -- for the no-overlap EXCLUDE constraints

-- ===== Reference data (small, cached) =====
CREATE TABLE measure (                     -- one row per measurable quantity
  measure_code   SMALLINT PRIMARY KEY,     -- 1 SYSTOLIC, 2 DIASTOLIC, 3 PULSE, 4 GLUCOSE, 5 SPO2, 6 WEIGHT
  name           TEXT NOT NULL UNIQUE,
  reading_type   SMALLINT NOT NULL,        -- 1 BLOOD_PRESSURE, 2 GLUCOSE, 3 PULSE_OX, 4 WEIGHT
  unit           TEXT NOT NULL,            -- canonical unit; ingest converts (mmol/L -> mg/dL)
  min_plausible  NUMERIC(8,2) NOT NULL,    -- device-error guard, e.g. systolic 40
  max_plausible  NUMERIC(8,2) NOT NULL,    -- e.g. systolic 300
  CHECK (min_plausible < max_plausible)
);

CREATE TABLE device (
  device_id      BIGINT PRIMARY KEY,
  serial_no      TEXT NOT NULL UNIQUE,
  device_type    TEXT NOT NULL CHECK (device_type IN ('BP_CUFF','GLUCOMETER','PULSE_OX','SCALE')),
  status         TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','SUSPENDED','RETIRED')),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE device_credential (           -- API keys; two can be active during rotation
  key_id         TEXT PRIMARY KEY,         -- public part of the key: vl_<key_id>_<secret>
  device_id      BIGINT NOT NULL REFERENCES device,
  secret_hash    BYTEA NOT NULL,           -- HMAC-SHA256(secret, server pepper in KMS); raw secret never stored
  status         TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','REVOKED')),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at     TIMESTAMPTZ
);
CREATE INDEX ix_credential_device ON device_credential (device_id) WHERE status = 'ACTIVE';

CREATE TABLE device_binding (              -- device <-> patient over time
  device_id      BIGINT NOT NULL REFERENCES device,
  patient_id     BIGINT NOT NULL,
  valid_from     TIMESTAMPTZ NOT NULL,
  valid_to       TIMESTAMPTZ,              -- NULL = current
  PRIMARY KEY (device_id, valid_from),
  CHECK (valid_to IS NULL OR valid_to > valid_from),
  EXCLUDE USING gist (device_id WITH =, tstzrange(valid_from, valid_to) WITH &&)  -- no overlapping owners
);
CREATE UNIQUE INDEX ux_binding_current ON device_binding (device_id) INCLUDE (patient_id) WHERE valid_to IS NULL;

-- ===== Projections owned by other services (kept in sync from their events) =====
CREATE TABLE enrollment (
  patient_id     BIGINT NOT NULL,
  program_id     BIGINT NOT NULL,
  valid_from     TIMESTAMPTZ NOT NULL,
  valid_to       TIMESTAMPTZ,
  PRIMARY KEY (patient_id, program_id, valid_from),
  EXCLUDE USING gist (patient_id WITH =, program_id WITH =, tstzrange(valid_from, valid_to) WITH &&)
);

CREATE TABLE care_assignment (             -- who looks after a patient; drives routing and access
  patient_id     BIGINT NOT NULL,
  coach_id       BIGINT NOT NULL,
  role           TEXT NOT NULL CHECK (role IN ('PRIMARY','BACKUP')),
  valid_from     TIMESTAMPTZ NOT NULL,
  valid_to       TIMESTAMPTZ,
  PRIMARY KEY (patient_id, coach_id, role, valid_from)
);
CREATE UNIQUE INDEX ux_primary_coach ON care_assignment (patient_id) INCLUDE (coach_id)
  WHERE role = 'PRIMARY' AND valid_to IS NULL;
CREATE INDEX ix_coach_panel ON care_assignment (coach_id, patient_id) WHERE valid_to IS NULL;

-- ===== Threshold rules: immutable, one rule per row =====
CREATE TABLE threshold_rule (
  threshold_id   BIGSERIAL PRIMARY KEY,    -- identifies one exact rule version
  patient_id     BIGINT,                   -- patient override ...
  program_id     BIGINT,                   -- ... or program default, never both
  measure_code   SMALLINT NOT NULL REFERENCES measure,
  operator       TEXT NOT NULL CHECK (operator IN ('GT','LT')),
  limit_value    NUMERIC(8,2) NOT NULL,
  severity       TEXT NOT NULL CHECK (severity IN ('CRITICAL','HIGH','MEDIUM')),
  confirm_n      SMALLINT NOT NULL DEFAULT 1,  -- open an episode after N breaches ...
  confirm_window INTERVAL,                     -- ... within this window (non-CRITICAL only)
  valid_from     TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_to       TIMESTAMPTZ,              -- a change closes this row and inserts a new one
  created_by     TEXT NOT NULL,
  CHECK ((patient_id IS NULL) <> (program_id IS NULL)),
  CHECK (severity <> 'CRITICAL' OR confirm_n = 1),   -- critical breaches always alert at once
  CHECK (confirm_n = 1 OR confirm_window IS NOT NULL)
);
CREATE UNIQUE INDEX ux_rule_patient ON threshold_rule (patient_id, measure_code, operator, severity)
  WHERE patient_id IS NOT NULL AND valid_to IS NULL;
CREATE UNIQUE INDEX ux_rule_program ON threshold_rule (program_id, measure_code, operator, severity)
  WHERE program_id IS NOT NULL AND valid_to IS NULL;

CREATE TABLE notification_policy (         -- per-patient delivery preference set by a clinician
  patient_id     BIGINT PRIMARY KEY,
  non_critical   TEXT NOT NULL DEFAULT 'IMMEDIATE' CHECK (non_critical IN ('IMMEDIATE','DAILY_DIGEST')),
  set_by         BIGINT NOT NULL,
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ===== Hot table: ~50M rows/day =====
CREATE TABLE reading (
  device_id      BIGINT      NOT NULL,
  reading_uuid   UUID        NOT NULL,    -- from the device; if missing, server derives UUIDv5(device_id, device_seq, measured_at, reading_type)
  measured_at    TIMESTAMPTZ NOT NULL,    -- device clock; partition key
  received_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  patient_id     BIGINT      NOT NULL,    -- owner at measured_at (binding history), not owner now
  reading_type   SMALLINT    NOT NULL,
  device_seq     BIGINT,
  systolic       SMALLINT, diastolic SMALLINT, pulse SMALLINT,
  value_num      NUMERIC(8,2),            -- glucose, SpO2, weight in the canonical unit
  payload_hash   BIGINT      NOT NULL,    -- 64-bit hash of the values: same id + different values = conflict
  extras         JSONB,
  PRIMARY KEY (device_id, reading_uuid, measured_at),
  CHECK (reading_type <> 1 OR (systolic IS NOT NULL AND diastolic IS NOT NULL)),
  CHECK (reading_type = 1 OR value_num IS NOT NULL)
) PARTITION BY RANGE (measured_at);
-- Daily partitions created 7 days ahead by pg_partman; DEFAULT partition catches clock-skewed rows.
CREATE INDEX ix_reading_patient_time ON reading (patient_id, reading_type, measured_at DESC, reading_uuid);

-- ===== Transactional outbox (one per database: ingest DB and alerting DB) =====
CREATE TABLE outbox_event (
  event_id       UUID PRIMARY KEY,         -- UUIDv7 generated by the application
  topic          TEXT NOT NULL,            -- chosen at write time; CRITICAL alerts go to alerts.raised.critical
  event_type     TEXT NOT NULL CHECK (event_type IN ('readings.ingested','alerts.raised',
                   'alert.state_changed','thresholds.changed','device.binding_changed')),
  aggregate_key  TEXT NOT NULL,            -- Kafka partition key: patient_id, or device_id for bindings
  schema_version SMALLINT NOT NULL DEFAULT 1,
  payload        JSONB NOT NULL,           -- IDs and values only, never names or contact details
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_outbox_created ON outbox_event (created_at);  -- local poller order; deleted after publish

CREATE TABLE processed_event (             -- consumer-side dedup where no natural key exists
  consumer       TEXT NOT NULL,
  event_id       UUID NOT NULL,
  processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
CREATE INDEX ix_processed_at ON processed_event (processed_at);  -- daily purge past Kafka retention + 1 day

-- ===== Alerts: one row per episode =====
CREATE TABLE alert (
  alert_id        BIGSERIAL PRIMARY KEY,
  patient_id      BIGINT NOT NULL,
  measure_code    SMALLINT NOT NULL REFERENCES measure,
  coach_id        BIGINT NOT NULL,         -- current owner; moved when the panel changes
  severity        TEXT NOT NULL CHECK (severity IN ('CRITICAL','HIGH','MEDIUM')),  -- highest in the episode
  severity_rank   SMALLINT GENERATED ALWAYS AS
                    (CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END) STORED,
  ai_priority     TEXT,                    -- advisory only, never changes severity
  status          TEXT NOT NULL DEFAULT 'OPEN'
                    CHECK (status IN ('OPEN','ACKNOWLEDGED','ESCALATED','RESOLVED')),
                                           -- ACKNOWLEDGED returns to OPEN when severity rises to CRITICAL
  is_late         BOOLEAN NOT NULL DEFAULT false,
  escalate_at     TIMESTAMPTZ,             -- set when the alert is OPEN and CRITICAL (now + 15 min);
                                           -- cleared on acknowledge, escalate or resolve
  first_seen_at   TIMESTAMPTZ NOT NULL,    -- measured_at of the first breach
  last_seen_at    TIMESTAMPTZ NOT NULL,    -- not indexed: repeat breaches stay HOT updates
  occurrence_cnt  INT NOT NULL DEFAULT 1 CHECK (occurrence_cnt >= 1),  -- not indexed, same reason
  version         INT NOT NULL DEFAULT 0,  -- optimistic lock for ack/resolve
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  acknowledged_by BIGINT, acknowledged_at TIMESTAMPTZ,
  resolved_by     BIGINT, resolved_at TIMESTAMPTZ, resolution_note TEXT,
  CHECK (last_seen_at >= first_seen_at),
  CHECK (status <> 'RESOLVED' OR resolved_at IS NOT NULL),
  CHECK (status <> 'ACKNOWLEDGED' OR acknowledged_at IS NOT NULL),
  CHECK (escalate_at IS NULL OR (status = 'OPEN' AND severity = 'CRITICAL'))  -- timer only where it applies
) WITH (fillfactor = 80);                  -- free space per page so repeat-breach updates stay on the page (HOT)
CREATE UNIQUE INDEX ux_alert_active_episode ON alert (patient_id, measure_code) WHERE status <> 'RESOLVED';
CREATE INDEX ix_alert_queue ON alert (coach_id, severity_rank, created_at) WHERE status <> 'RESOLVED';
CREATE INDEX ix_alert_escalation ON alert (escalate_at) WHERE escalate_at IS NOT NULL;

CREATE TABLE alert_occurrence (            -- each breaching reading folded into an episode (~10M rows/day at 20%)
  device_id       BIGINT NOT NULL,
  reading_uuid    UUID NOT NULL,
  measure_code    SMALLINT NOT NULL,
  measured_at     TIMESTAMPTZ NOT NULL,    -- partition key; with device_id + reading_uuid = the reading's key
  alert_id        BIGINT NOT NULL REFERENCES alert,
  observed_value  NUMERIC(8,2) NOT NULL,
  threshold_id    BIGINT NOT NULL REFERENCES threshold_rule,  -- the exact rule version that fired
  severity        TEXT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (device_id, reading_uuid, measure_code, measured_at)  -- alert idempotency key; a replay has the same measured_at
) PARTITION BY RANGE (measured_at);
-- Daily partitions by pg_partman, dropped after 90 days like reading; the episode keeps occurrence_cnt.
CREATE INDEX ix_occurrence_alert ON alert_occurrence (alert_id, measured_at);

CREATE TABLE notification (
  notification_id  BIGSERIAL PRIMARY KEY,
  trigger_event_id UUID NOT NULL,          -- the alerts.raised / alert.state_changed event that caused it
  alert_id         BIGINT NOT NULL REFERENCES alert,
  coach_id         BIGINT NOT NULL,
  channel          TEXT NOT NULL CHECK (channel IN ('QUEUE','PUSH','SMS','DIGEST')),
  reason           TEXT NOT NULL CHECK (reason IN ('RAISED','SEVERITY_UPGRADED','ESCALATED','REASSIGNED')),
  severity         TEXT NOT NULL,          -- severity at the time; shown in the message
  status           TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','SENT','FAILED')),
  attempts         INT NOT NULL DEFAULT 0,
  next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at          TIMESTAMPTZ,
  UNIQUE (trigger_event_id, coach_id, channel)                 -- delivery idempotency: one per change, coach, channel
);
CREATE INDEX ix_notification_alert ON notification (alert_id);
CREATE INDEX ix_notification_due ON notification (channel, next_attempt_at) WHERE status = 'PENDING';

-- ===== Audit: append-only PHI access trail =====
CREATE TABLE audit_log (
  audit_id       UUID NOT NULL,            -- UUIDv7
  at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  actor_type     TEXT NOT NULL CHECK (actor_type IN ('DEVICE','COACH','SERVICE','ADMIN')),
  actor_id       TEXT NOT NULL,
  action         TEXT NOT NULL,            -- READ_READINGS, ACK_ALERT, UPDATE_THRESHOLD ...
  outcome        TEXT NOT NULL CHECK (outcome IN ('ALLOWED','DENIED')),
  patient_id     BIGINT,
  resource       TEXT NOT NULL,
  request_id     UUID NOT NULL,
  PRIMARY KEY (audit_id, at)
) PARTITION BY RANGE (at);
CREATE INDEX ix_audit_patient ON audit_log (patient_id, at);
-- The service role gets INSERT only on audit_log (no UPDATE/DELETE); partitions are exported daily to WORM storage.

-- ===== Database placement at scale =====
-- Ingest DB:   device, device_credential, device_binding, measure, reading, outbox_event, audit_log
-- Alerting DB: measure, threshold_rule, enrollment, care_assignment, notification_policy, alert,
--              alert_occurrence, notification, outbox_event, processed_event, audit_log
-- Both later sharded by patient_id (device tables by device_id) so a patient's rows stay on one shard.
```

**Why these choices**

- `PRIMARY KEY (device_id, reading_uuid, measured_at)`: Postgres needs the partition key inside any unique constraint. A retry resends the same `measured_at`, so the key still catches duplicates. `payload_hash` catches the rare retry that reuses an id with different values; ingest answers `CONFLICT` for that item and logs an integrity event.
- Narrow typed columns (`SMALLINT`) for the hot metrics keep a row near 90 bytes; `JSONB extras` avoids schema churn for rare vendor fields.
- **Alert = episode, occurrence = reading.** One unresolved alert per patient and measure (`OPEN`, `ACKNOWLEDGED` or `ESCALATED`). Each breaching reading adds an `alert_occurrence` row, raises `occurrence_cnt`, and upgrades `severity` if higher. The occurrence primary key is the alert idempotency key.
- **Threshold rules are immutable.** A change closes the old row and inserts a new one, so `threshold_id` names the exact rule that fired and old alerts stay explainable.
- **Severity comes from the value.** Each rule is one band (`GT 180 = CRITICAL`, `GT 160 = HIGH`); the evaluator takes the most severe band the value crosses. A patient override replaces the program default for that measure; with several programs and no override, the most conservative default wins.
- **No foreign key from alerts to readings.** Reading partitions are dropped after 90 days, and a foreign key would block that. Alerts store the reading's full key instead.

### Schema validation: gaps found and fixed

| # | Gap in the first schema | Risk | Fix |
| --- | --- | --- | --- |
| 1 | API key stored as a salted argon2id hash | No way to look a key up by hash, so every request scans or fails; slow hashing per request | `device_credential` with public `key_id` + HMAC-SHA256 secret; two active keys during rotation |
| 2 | Binding history could overlap; index needed a table read | A reading attributed to the wrong patient | `EXCLUDE` constraint on the validity range; `INCLUDE (patient_id)`; binding checked at `measured_at` |
| 3 | Threshold had no uniqueness, unclear `min`/`max` meaning, updated in place | Conflicting rules; old alerts not explainable | `threshold_rule`: one band per row, `operator` + `limit_value`, partial unique indexes, patient-XOR-program check, immutable versions |
| 4 | Metric names mixed `SMALLINT` (reading) and `TEXT` (threshold, alert); one BP reading holds three measures | Joins and caches keyed inconsistently | `measure` reference table; thresholds and alerts use `measure_code` |
| 5 | Open-episode index covered `OPEN` only | Acknowledged or escalated alert plus a new breach = duplicate alert | Unique on `(patient_id, measure_code)` for every unresolved status |
| 6 | Severity was part of the episode key | HIGH and CRITICAL open at once for one measure | One episode per measure; severity upgraded in place |
| 7 | Two unique keys on `alert`, one `ON CONFLICT` target; coalescing lost which readings breached | Replay could double-count; no audit of breaches | `alert_occurrence` keyed by reading + measure; a conflict rolls back the whole evaluator transaction |
| 8 | Notification key `(alert, coach, channel)`, later `+ reason + severity` | Upgrades could never re-notify; a second escalation after a reopen was still blocked | Key on the triggering event: `UNIQUE (trigger_event_id, coach_id, channel)` (see #21) |
| 9 | Queue sorted by severity text | Correct only by alphabet luck | Generated `severity_rank` in the queue index |
| 10 | No local source for coach, backup coach or panel | Routing, escalation and access checks had nothing to read | `care_assignment` projection with one current primary coach |
| 11 | Patients in several programs | Ambiguous default thresholds | `enrollment` projection; override wins, else most conservative default |
| 12 | `event_id` from a sequence | Not in commit order; collides across shards | UUIDv7 from the application; `processed_event.event_id` is `UUID` |
| 13 | Same reading id with different values silently dropped | Lost data or hidden device fault | `payload_hash` compared on conflict |
| 14 | Keyset pagination tie-breaker not in the index | Extra sort per page | `reading_uuid` added to `ix_reading_patient_time` |
| 15 | No `CHECK`s on enum text and per-type values | Bad data enters silently | `CHECK` constraints; plausibility ranges in `measure` |
| 16 | `processed_event` grew forever | Unbounded table and index | Daily purge past Kafka retention, with an index on `processed_at` |
| 17 | `audit_log` had no key, could be updated | Weak audit evidence | Primary key, `outcome` (denials logged), INSERT-only grants, WORM export |
| 18 | Notification retries had no due-time index | Retry scan grows with the table | `next_attempt_at` + partial index on `PENDING` |
| 19 | Readings older than the 90-day hot window | Insert into a dropped range lands in `DEFAULT` | Rejected with `422 READING_TOO_OLD` and counted; assumption to confirm with clinical owners |
| 20 | Escalation timer measured from `created_at` | An alert raised as HIGH and upgraded to CRITICAL an hour later escalates instantly | `escalate_at` set when the alert becomes OPEN + CRITICAL, cleared on ack/escalate/resolve; `CHECK` keeps it only where it applies; index on `escalate_at` |
| 21 | Notification key could not tell two real changes of the same kind apart | Escalated, acknowledged, reopened, escalated again = second page silently dropped | `trigger_event_id` in the key: each change has its own event id; a replay reuses it |
| 22 | No rule for a severity upgrade on an acknowledged alert | Coach acknowledged a HIGH and is never prompted for the CRITICAL | Upgrade to CRITICAL moves `ACKNOWLEDGED` back to `OPEN`, sets `escalate_at`, and notifies; clinical sign-off needed |
| 23 | alert\_occurrence not partitioned | \~10M rows/day at a 20% breach rate, no cheap way to expire them | Daily partitions on measured\_at, 90-day retention; measured\_at added to the primary key |
| 24 | Every repeat breach rewrote the alert row and its indexes | Index churn and vacuum load at thousands of updates per second | fillfactor = 80; repeat breaches change only unindexed columns, so updates stay HOT |
| 25 | One topic for all alert severities | CRITICAL alerts queue behind HIGH and MEDIUM during a spike | outbox\_event.topic chosen at write time; alerts.raised.critical with its own notifier group |
| 26 | No way to hold back non-critical alert volume | A 20% breach rate floods coaches | confirm\_n / confirm\_window on threshold\_rule (never for CRITICAL), notification\_policy with a DIGEST channel |
| 27 | `alert_occurrence.severity` and `notification.severity` had no CHECK | A typo such as 'LOW' or 'high' is stored silently; the queue and the message show a severity that does not exist | `CHECK (severity IN ('CRITICAL','HIGH','MEDIUM'))` on both tables |
| 28 | `ix_alert_queue` had no tie-breaker column | Alerts with the same severity and created\_at page unstably: rows skipped or repeated between queue pages, plus an extra sort per page | `alert_id` added as the last index column; queue cursor = `(severity_rank, created_at, alert_id)`. alert\_id never changes, so repeat-breach updates stay HOT |
| 29 | `care_assignment` had no no-overlap constraint, unlike device\_binding | Two primary coaches at once: alerts routed to the wrong coach, panel access granted to both | `EXCLUDE` on (patient\_id, role, validity range) plus `CHECK (valid_to > valid_from)`: one PRIMARY and one BACKUP per patient at any moment; the projection consumer closes the old row before inserting the new one |
| 30 | `measure.reading_type` tied each measure to one reading type; PULSE belonged only to blood pressure | A pulse oximeter's pulse value had no measure, so it was never evaluated or alerted on | `reading_type_measure` maps each reading type to its measures and source column; PULSE comes from BP cuffs and oximeters and shares one episode and its thresholds (clinical sign-off needed) |
| 31 | Seeded threshold bands are illustrative, not clinically approved | Demo values could reach production and drive real alerts | Seed rows use `created_by = 'SEED-NOT-CLINICAL'`; the prod profile refuses to start if any exist; real bands need clinical sign-off |

Gaps 27–31 were found while applying the schema in stage 2 and are fixed by migration `V202611080900__schema_fixes_after_stage2.sql`; the schema listing above shows the stage 2 starting point.

### Interfaces (Java)

```java
// Write path
public interface ReadingIngestService {
  IngestResult ingest(DeviceIdentity device, IngestBatch batch);   // per item: ACCEPTED | DUPLICATE | CONFLICT | REJECTED
}
public interface DeviceAuthenticator {
  DeviceIdentity authenticate(String apiKey);                      // "vl_<keyId>_<secret>": key_id lookup, constant-time HMAC compare, cached
  void assertBoundTo(DeviceIdentity d, long patientId, Instant measuredAt); // binding at measured_at; DeviceMismatchException -> 403
}
public interface ReadingRepository {
  List<UUID> insertIgnoringDuplicates(List<Reading> batch);         // INSERT ... ON CONFLICT DO NOTHING RETURNING reading_uuid
  List<ExistingHash> findPayloadHashes(long deviceId, List<ReadingKey> keys); // DUPLICATE vs CONFLICT
  Page<Reading> findRange(long patientId, ReadingType type, Instant from, Instant to, Cursor c, int limit); // keyset, never OFFSET
}
public interface OutboxWriter { void append(List<OutboxEvent> events); } // same transaction; event_id = UUIDv7; topic set per event
public interface LatestReadingCache {
  void putIfNewer(long patientId, ReadingType type, Reading r);     // Lua compare-and-set on measured_at; best effort
  Optional<Reading> get(long patientId, ReadingType type);
}

// Alert path
public interface MeasureEvaluator {                                 // Strategy, one per reading type
  ReadingType supports();
  List<Breach> evaluate(Reading r, EffectiveThresholds t);          // most severe band crossed per measure_code
}
public interface ThresholdProvider { EffectiveThresholds forPatients(List<Long> patientIds); }
// CachingThresholdProvider decorates DbThresholdProvider (Decorator); override > program default; several programs -> most conservative
public interface AlertService {
  BatchOutcome raiseBatch(List<BreachWithReading> breaches);        // one transaction per batch; DuplicateKeyException -> per-event fallback
  Optional<AlertChange> raise(BreachWithReading breach);           // per-event path; duplicate occurrence = already evaluated
  Alert acknowledge(long alertId, long coachId, int expectedVersion); // optimistic lock; 409 on mismatch
  Alert resolve(long alertId, long coachId, int expectedVersion, String note); // note required for CRITICAL
}
public interface EscalationScheduler { int escalateDue(Instant now, int limit); } // SKIP LOCKED claim; escalates each alert once
public interface NotificationService { void record(AlertEvent event); } // ON CONFLICT (trigger_event_id, coach_id, channel) DO NOTHING
public interface NotificationSender { int sendDue(Channel channel, int limit); } // PENDING -> SENT; provider key = notification_id

// AI (off the critical path)
public interface LlmClient { CompletableFuture<String> complete(Prompt p, Duration timeout); }
public interface ReadingSummaryService { Summary summarize(long patientId, Duration window); } // falls back to a rule-based summary
```

Patterns used where they pay off: **Strategy** for measure evaluators, one per reading type (new device types add a class, not an `if`), **Decorator** for caching, **Transactional Outbox** for reliable events, **Circuit Breaker** (Resilience4j) around the LLM and Redis. No generic base repositories or factories for their own sake.

## Alert processing

An alert passes through three transactions: evaluate a reading into an episode, change the episode's state, and turn each change a coach must see into a notification. Database keys make every step a no-op on replay, so Kafka's at-least-once delivery never produces a second alert or a second message.

### End to end: raise and escalation

> Diagram: alert raise and escalation, end to end · 8 duplicate guards. See the [live design doc](https://claude.ai/artifact/CBKPwbpYbhHwWhCgCTvBhi) for the drawing.

Every arrow is at-least-once: devices retry, the relay can re-publish, Kafka can redeliver, and the scheduler runs on several pods. Each numbered guard turns a repeat into a no-op.

| Guard | Where | Key or check | A repeat here results in |
| --- | --- | --- | --- |
| 1 | Ingest insert | Reading PK `(device_id, reading_uuid, measured_at)`; outbox row only for rows `RETURNING` says are new | Stored once, no second `readings.ingested` |
| 2 | Outbox relay | Same UUIDv7 `event_id` on re-publish; idempotent producer | Same event id downstream, absorbed by guard 4 |
| 3 | Evaluator, episode | Per-patient Kafka key; `FOR UPDATE` on the episode; unique active episode `(patient_id, measure_code)` | Folded into the existing alert, never a second alert |
| 4 | Evaluator, reading | `alert_occurrence` PK `(device_id, reading_uuid, measure_code, measured_at)` | Whole transaction rolls back: no count change, no new `alerts.raised` |
| 5 | Notification record | `UNIQUE (trigger_event_id, coach_id, channel)` | No second row for the same change |
| 6 | External send | `PENDING → SENT` on the row; provider idempotency key = `notification_id` | No second push or SMS |
| 7 | Escalation | `SKIP LOCKED` claim + `status = 'OPEN'` guard; `escalate_at` cleared in the same update; `CHECK` keeps the timer only on OPEN + CRITICAL | Escalated once; other pods skip the row |
| 8 | Coach action | `version` match; acknowledge clears `escalate_at` | Stale click gets `409`; acknowledged alert can no longer escalate |

| What repeats or races | Guards that absorb it |
| --- | --- |
| Device resends a batch after a timeout | 1 |
| Relay crashes after publishing, before deleting the outbox row | 2, then 4 |
| Evaluator commits, crashes before committing the Kafka offset | 4 |
| Kafka rebalance gives one partition to two consumers briefly | 3, 4 |
| Notifier redelivered `alerts.raised` | 5, 6 |
| Three scheduler pods wake at the same second | 7 |
| Coach acknowledges while the scheduler is escalating | 7 + 8: whichever commits first wins; the other sees a changed status or version (the coach gets `409`, then can acknowledge the `ESCALATED` alert) |

### Entities involved

| Entity | Role in the flow |
| --- | --- |
| `reading` + `outbox_event` | Input: new readings only, announced as `readings.ingested` |
| `measure`, `threshold_rule`, `enrollment` | Split a reading into measures and find the bands that apply |
| `care_assignment` | Who gets the alert: primary coach, backup on escalation |
| `alert` | The episode: status, severity, count, `escalate_at`, `version` |
| `alert_occurrence` | One row per breaching reading; the alert idempotency key |
| `notification` | One row per change, coach and channel; the delivery idempotency key |
| `audit_log` | Every state change with actor and request |

### 1. Evaluate a reading into an episode

The evaluator consumes `readings.ingested` (keyed by `patient_id`, so one patient is handled in order), splits each reading into measures, and takes the most severe band each value crosses. A value inside all bands does nothing; alerts never resolve themselves. Each breach runs this transaction (shown per breach for clarity; in production the same steps run set-based once per batch of up to 500 events, see Scaling alert processing):

```sql
BEGIN;
-- a) cheap early exit for replays (the PK in step d is the guarantee)
SELECT 1 FROM alert_occurrence WHERE device_id=$d AND reading_uuid=$r AND measure_code=$m;

-- b) lock this patient's unresolved episode for the measure, if any
SELECT alert_id, status, severity FROM alert
WHERE patient_id=$p AND measure_code=$m AND status <> 'RESOLVED'
FOR UPDATE;

-- c1) none: open a new episode
INSERT INTO alert (patient_id, measure_code, coach_id, severity, is_late,
                   escalate_at, first_seen_at, last_seen_at)
VALUES ($p, $m, $primary_coach, $sev, $late,
        CASE WHEN $sev = 'CRITICAL' THEN now() + interval '15 minutes' END, $t, $t);

-- c2) found: fold the breach in; an upgrade to CRITICAL reopens and starts the timer
UPDATE alert SET occurrence_cnt = occurrence_cnt + 1,
       last_seen_at = GREATEST(last_seen_at, $t),
       severity     = $higher_severity,
       status       = CASE WHEN $upgraded_to_critical AND status = 'ACKNOWLEDGED' THEN 'OPEN' ELSE status END,
       escalate_at  = CASE WHEN $upgraded_to_critical AND status IN ('OPEN','ACKNOWLEDGED')
                           THEN now() + interval '15 minutes' ELSE escalate_at END,
       version      = version + 1
WHERE alert_id = $id;

-- d) record the breaching reading: PK conflict = already evaluated -> ROLLBACK all of it
INSERT INTO alert_occurrence (device_id, reading_uuid, measure_code, measured_at,
                              alert_id, observed_value, threshold_id, severity) VALUES (...);

-- e) only a new episode or a severity upgrade is announced
INSERT INTO outbox_event (event_id, topic, event_type, aggregate_key, payload)
VALUES ($uuidv7,
        CASE WHEN $sev = 'CRITICAL' THEN 'alerts.raised.critical' ELSE 'alerts.raised' END,
        'alerts.raised', $p, '{"alert_id":..., "reason":"RAISED" | "SEVERITY_UPGRADED", ...}');
COMMIT;   -- then commit the Kafka offset
```

| DB check | Stops | How |
| --- | --- | --- |
| `alert_occurrence` PK `(device_id, reading_uuid, measure_code, measured_at)` | One reading counted twice (redelivery, relay re-publish, consumer restart) | Conflict at step d rolls back step c too; a replay after the episode was resolved would open a new alert, and the rollback removes it |
| `ux_alert_active_episode` on `(patient_id, measure_code)` where unresolved | Two alerts for one episode | Normally prevented by the lock in step b; if two consumers race during a rebalance, the second insert fails, retries, and takes the update path |
| `FOR UPDATE` on the episode | Lost updates to count or severity | Writers to one alert queue behind each other |
| `CHECK (escalate_at IS NULL OR (status = 'OPEN' AND severity = 'CRITICAL'))` | A timer left running on an acknowledged, escalated or resolved alert | The database rejects any write that forgets to clear it |
| `CHECK`s on status, severity, timestamps | Invalid data from bugs | Rejected at write time |

A repeat breach at the same or lower severity only raises the count: no event, so the coach sees "×4" in the queue instead of four alerts.

### 2. State changes

> Diagram: alert state machine · 4 states, 3 notifying transitions. See the [live design doc](https://claude.ai/artifact/CBKPwbpYbhHwWhCgCTvBhi) for the drawing.

The unresolved states share one episode; a new breach after `RESOLVED` opens a new alert instead of reopening the old one.

| Transition | Trigger | Guard in the `UPDATE` | Also clears |
| --- | --- | --- | --- |
| — → `OPEN` | First breach (evaluator) | Unique active episode | — |
| `OPEN` → `ACKNOWLEDGED` | Coach | `version = $expected AND status IN ('OPEN','ESCALATED')` | `escalate_at` |
| `OPEN` → `ESCALATED` | Scheduler when `escalate_at <= now()` | `status = 'OPEN'`, row claimed with `SKIP LOCKED` | `escalate_at` |
| `ESCALATED` → `ACKNOWLEDGED` | Primary or backup coach | `version` match | — |
| `ACKNOWLEDGED` → `OPEN` | Severity rises to CRITICAL (evaluator) | Episode row lock | sets `escalate_at` |
| any unresolved → `RESOLVED` | Coach; note required for CRITICAL | `version` match | `escalate_at` |
| any unresolved, new `coach_id` | Panel change from `care_assignment` | `status <> 'RESOLVED'` | — |

Every coach action is one conditional update; zero rows updated means someone else changed it first, and the API answers `409` with the current state:

```sql
UPDATE alert
SET status = 'ACKNOWLEDGED', acknowledged_by = $coach, acknowledged_at = now(),
    escalate_at = NULL, version = version + 1
WHERE alert_id = $id AND version = $expected AND status IN ('OPEN','ESCALATED')
RETURNING *;
-- same transaction: INSERT audit_log; INSERT outbox_event 'alert.state_changed'

-- escalation scheduler, safe on many pods
UPDATE alert SET status = 'ESCALATED', escalate_at = NULL, version = version + 1
WHERE alert_id IN (SELECT alert_id FROM alert WHERE escalate_at <= now()
                   ORDER BY escalate_at LIMIT 100 FOR UPDATE SKIP LOCKED)
  AND status = 'OPEN'
RETURNING alert_id;   -- one 'alert.state_changed' outbox row per alert
```

### 3. Changes into notifications

The notification worker turns each change into one row per coach and channel, keyed by the event that caused it. Non-critical new episodes and upgrades arrive on `alerts.raised` (group `notifier`). CRITICAL ones arrive on `alerts.raised.critical`, and escalations and reassignments on `alert.state_changed`; both are consumed by group `notifier-critical`, each topic in its own container, so an escalation page never waits behind a non-critical backlog. Recipients are read from `care_assignment` when the event is recorded (PRIMARY, plus BACKUP for escalations). With no open PRIMARY, the alert's owner (`coach_id` in the event) is notified, so a change is never dropped:

| Change | Reason | To | Channels |
| --- | --- | --- | --- |
| New episode | `RAISED` | Primary coach | QUEUE; + PUSH if CRITICAL |
| Severity raised | `SEVERITY_UPGRADED` | Primary coach | QUEUE; + PUSH if CRITICAL |
| Escalated | `ESCALATED` | Backup coach and primary | QUEUE, PUSH, SMS |
| Reassigned | `REASSIGNED` | New coach | QUEUE |
| Acknowledged, resolved, repeat breach | none | — | Audit and queue update only |

```sql
INSERT INTO notification (trigger_event_id, alert_id, coach_id, channel, reason, severity)
VALUES ($event_id, $alert_id, $coach, $channel, $reason, $severity)
ON CONFLICT (trigger_event_id, coach_id, channel) DO NOTHING;   -- replay = no-op; a new change = new event id
```

- QUEUE rows are marked `SENT` at once; the queue itself reads the `alert` table.
- PUSH and SMS: a sender claims due `PENDING` rows with `FOR UPDATE SKIP LOCKED` (CRITICAL first), calls the provider with `notification_id` as its idempotency key under a timeout and a circuit breaker, and marks `SENT`. Failures raise `attempts` and push `next_attempt_at` out with backoff (5 s doubling, capped at 5 min, with jitter). This retry is durable, so it survives a restart. After 5 calls the row is `FAILED` and ops are paged. An open circuit makes no call and uses no attempt. While the push circuit is open, an SMS row is recorded for the same change and coach and sent by the SMS sender through the SMS provider (a dummy `SMS_NOTIFIER` here). The push itself stays pending and goes out when the circuit closes, so a push outage never leaves a CRITICAL unannounced.
- `DIGEST` rows are written only when the clinician-set `DAILY_DIGEST` policy and the `digest-enabled` flag (off by default) are both on, and never for CRITICAL or escalations.
- The row makes the *decision* to notify exactly-once; the provider key makes the send effectively-once. Without provider support the worst case is one duplicate push, never a missed one.
- The reopen-on-CRITICAL rule is a clinical policy and needs sign-off from the clinical lead.

## Performance and scaling

We size for the morning burst, not the daily average: 50M readings/day averages \~580/s, but the 6–9 AM window peaks near 15,000 readings/s. The write path is built so the database does one multi-row insert per batch and nothing else synchronous.

### Capacity model (estimates to validate with load tests)

| Quantity | Estimate | Basis |
| --- | --- | --- |
| Daily average | \~580 readings/s | 50M ÷ 86,400 |
| Morning sustained | \~2,800 readings/s | Assume 60% of volume in 6–9 AM |
| Morning spike | \~15,000 readings/s | 5× sustained at top-of-hour reconnect bursts |
| Requests at spike | \~3,000 req/s | Average batch of 5 readings |
| Storage, hot | \~8 GB/day incl. indexes | \~80 B row + \~80 B index per reading |
| Hot retention | 90 days ≈ 720 GB | Older data archived (below) |
| Breaches at morning spike | \~3,000/s | Planning assumption: up to 20% of readings breach a threshold |
| Breaches per day | \~10M | 20% of 50M; \~1–1.5 GB/day of alert\_occurrence rows |

### Ingest path

- **Batch-first writes**: one `INSERT … ON CONFLICT DO NOTHING RETURNING` per request (up to 500 rows) plus one outbox insert, in one short transaction. No per-row round trips.
- **Java 21 virtual threads** for request handling; HikariCP pool kept small (\~20 per pod) behind **PgBouncer** in transaction mode, so pods scale without exhausting Postgres connections.
- **Fast rejection before the DB**: auth, schema validation, plausibility ranges (systolic 40–300) and per-device rate limits run in memory or Redis.
- **Redis is best-effort on ingest**: the latest-reading update runs after commit and never fails the request; a Redis outage only costs cache freshness.
- **Horizontal scaling**: ingest-api is stateless; HPA scales on CPU and in-flight requests, with a pre-warm schedule from 5:30 AM because the burst is predictable.

### Alert pipeline

- `readings.ingested` has **128 partitions** keyed by `patient_id`, sized now because adding partitions later moves patients between partitions and breaks per-patient order during the change. Evaluator pods scale on consumer lag (KEDA) up to 128, pre-scaled from 5:30 AM.
- Consumers poll up to 500 events and evaluate them **set-based in one transaction per batch** (below); thresholds come from an in-process cache, then one Redis `MGET`, then one DB query for misses.
- Only new episodes and severity upgrades go downstream. CRITICAL ones go to their own topic, `alerts.raised.critical`, with a dedicated notifier group (`notifier-critical`). That group also consumes `alert.state_changed`, which carries escalations, so neither waits behind lower severities.
- Separate consumer groups for evaluation, notification and AI triage, so a slow LLM or SMS provider never delays alerts.

### Scaling alert processing

At the planning assumption that 20% of readings breach, the morning spike brings \~3,000 breaches/s. One transaction per breach would add \~3,000 commits/s to the ingest primary and make it the bottleneck. Batching, in-place updates, partitioning, a critical lane and a separate alerting database bring that to a few hundred commits/s without relaxing any duplicate guard.

| At \~3,000 breaches/s | One transaction per breach | Batched (this design) |
| --- | --- | --- |
| Evaluator commits/s | \~3,000 | \~300–600 (64–128 consumers × \~5 batches/s) |
| Statements/s | \~13,000 | \~2,000–3,000 |
| Index writes per repeat breach | 3 (every index on `alert`) | 0 (HOT update) |
| Load on the ingest primary | Shared with ingest | None: alerting has its own primary |
| Critical alert to coach queue | Grows with lag during spikes | Batch commit ≤ 200 ms; separate critical lane |

Estimates to confirm with the load test, which now also replays the 6 AM curve at a 20% breach rate.

#### 1. Set-based batch evaluation

One Kafka partition belongs to one consumer, so a batch holds each of its patients' readings in order. The same guards apply, once per batch instead of once per breach:

```sql
BEGIN;
-- a) drop breaches already evaluated (guard 4), one query
SELECT o.device_id, o.reading_uuid, o.measure_code
FROM alert_occurrence o
JOIN unnest($dev::bigint[], $uuid::uuid[], $measure::smallint[], $at::timestamptz[]) b(d, r, m, t)
  ON o.device_id = b.d AND o.reading_uuid = b.r AND o.measure_code = b.m AND o.measured_at = b.t;

-- b) lock every unresolved episode the batch touches (guard 3), one query
SELECT a.alert_id, a.patient_id, a.measure_code, a.status, a.severity
FROM alert a
JOIN unnest($pat::bigint[], $pm::smallint[]) k(p, m) ON a.patient_id = k.p AND a.measure_code = k.m
WHERE a.status <> 'RESOLVED'
FOR UPDATE OF a;

-- c) in the app: merge breaches per (patient, measure); decide new / fold / upgrade

-- d) write every decision set-based
INSERT INTO alert (patient_id, measure_code, coach_id, severity, escalate_at,
                   first_seen_at, last_seen_at, occurrence_cnt)
SELECT * FROM unnest(...)                                    -- new episodes
RETURNING alert_id, patient_id, measure_code;

UPDATE alert a
SET occurrence_cnt = a.occurrence_cnt + v.n,
    last_seen_at   = GREATEST(a.last_seen_at, v.last_seen),
    severity = v.severity, status = v.status, escalate_at = v.escalate_at,  -- unchanged unless upgraded
    version  = a.version + 1
FROM unnest(...) v(alert_id, n, last_seen, severity, status, escalate_at)
WHERE a.alert_id = v.alert_id;                               -- folded breaches and upgrades

INSERT INTO alert_occurrence (...) SELECT * FROM unnest(...);  -- PK conflict -> ROLLBACK
INSERT INTO outbox_event (event_id, topic, event_type, aggregate_key, payload)
SELECT * FROM unnest(...);                                   -- new episodes and upgrades only
COMMIT;   -- then commit the batch's Kafka offsets
```

- A batch commits when it reaches 500 events or 200 ms, whichever comes first, so a quiet partition still delivers critical alerts fast.
- If the occurrence insert or the new-episode insert hits a unique conflict (only possible during a rebalance), the batch rolls back and is replayed one event at a time on the per-event path. Correctness never depends on the batch path.
- Several breaches for the same patient and measure in one batch become one row update with `n` added to the count.

#### 2. Cheaper writes

- `alert` uses `fillfactor = 80`, and a repeat breach changes only unindexed columns (`occurrence_cnt`, `last_seen_at`, `version`). Postgres then updates in place (a HOT update) with no index writes and little vacuum work. Severity and status, which are indexed, change rarely.
- `alert_occurrence` is partitioned by day on `measured_at` and dropped after 90 days, like `reading`. `measured_at` joins its primary key, and a replay carries the same value, so duplicate detection still works.
- Durability is not traded for speed: `synchronous_commit` stays on for alerting. Batching reduces the number of commits instead.

#### 3. Critical lane and notification throughput

- The evaluator writes the outbox `topic` per event; CRITICAL alerts go to `alerts.raised.critical`. Escalations are published on `alert.state_changed`. Both topics are consumed by the `notifier-critical` pods, each topic in its own container, with their own lag alert.
- PUSH and SMS senders use a token bucket per provider, kept in each pod, so the rate is configured per pod. Extra rows wait as `PENDING` and drain at the provider's rate. CRITICAL keeps a reserved share of each provider's quota (20% for PUSH, 30% for SMS); the critical pods' senders claim CRITICAL rows only.
- `ix_notification_due` is indexed by channel, so each channel's sender reads only its own queue.

#### 4. Separate, then shard, the database

1. **Now**: one Postgres primary, with alerting tables in their own schema.
2. **When evaluator commits pass \~30% of primary capacity**: move alerting tables to their own primary (placement listed under the schema). Each database keeps its own outbox, so no transaction ever spans both.
3. **At full target**: shard both by `patient_id` (Citus or application routing). Every evaluator query is patient-scoped, so each batch transaction stays on one shard; a batch spanning shards is split per shard before writing.

#### 5. Alert volume a coach can act on

A 20% breach rate is \~10M breaches/day. Episode coalescing turns that into far fewer alerts, but even 5% becoming new episodes would be \~500,000 alerts/day, beyond any coaching team. These controls are built in; turning each one on is a clinical decision:

| Control | How it works | Guard rail |
| --- | --- | --- |
| Confirmation rule | Open a non-critical episode only after `confirm_n` breaches within `confirm_window` (counted in Redis per patient and measure) | CRITICAL always alerts at once (`CHECK`); if Redis is down, alert immediately |
| Per-patient baselines | Clinician-approved patient overrides in `threshold_rule`, suggested from each patient's recent range | Every override records who set it |
| Daily digest | `notification_policy` sends non-critical alerts as one `DIGEST` per day for chronically out-of-range patients | Critical alerts and escalations are never digested |
| Coach load metric | New episodes per coach per day on the dashboard, with an alert above agreed capacity | Triggers panel rebalancing or threshold review |

### Data at scale

- **Partitioning**: daily range partitions on `measured_at` via pg\_partman, created 7 days ahead. Queries for a patient over a window touch only those days' partitions.
- **Retention**: after 90 days, partitions are detached, exported to Parquet on S3 (encrypted, Object Lock), and dropped; S3 lifecycle moves them to Glacier after 1 year. Retention length follows legal policy (assume 7 years).
- **Rollups**: an hourly/daily aggregate table (min, max, avg, count per patient per measure) serves long-range trend charts and the AI summary without scanning raw rows.
- **Sharding when one primary is not enough**: distribute by `patient_id` (Citus, or app-level hash routing). Every hot query is patient-scoped, so no cross-shard joins.
- **Read path**: query-api reads from replicas; keyset pagination on `(measured_at, reading_uuid)` instead of `OFFSET`.

### Caching and invalidation

| Key | Holds | Write strategy | Invalidation |
| --- | --- | --- | --- |
| `latest:{patientId}:{readingType}` | Latest reading | Write-through after commit, Lua compare-and-set on `measured_at` | Never stale-overwritten; TTL 7 days |
| `thr:{patientId}:{measureCode}` | Threshold set | Cache-aside, TTL 10 min | `DEL` on update + `thresholds.changed` event evicts the in-process Caffeine L1 |
| `dev:{keyHash}` | Device identity + current binding | Cache-aside, TTL 5 min | Evicted on key rotation, revocation or rebinding |
| `idem:{deviceId}:{readingUuid}` | Seen marker | `SET NX` with 24 h TTL | Expiry only; DB unique key is the real guard |
| `summary:{patientId}` | AI summary | Cache-aside, TTL 15 min | Evicted when a new alert is raised |

### Load testing

A k6 script fires 200 virtual devices, each sending 50-reading batches with 10% deliberate retries. We record throughput, p50/p95/p99 latency, duplicate rate and consumer lag, and expect the bottleneck to move from Postgres WAL/IOPS to Kafka producer throughput as batch size grows.

## Concurrency, idempotency and failure handling

Correctness rests on database constraints, not on application locks: every stage is at-least-once, and every write is keyed so a replay is a no-op. That gives effectively-once readings, alerts and notifications without distributed transactions.

### Idempotency chain

1. **Ingest**: `INSERT … ON CONFLICT (device_id, reading_uuid, measured_at) DO NOTHING RETURNING reading_uuid`. Only newly inserted rows get an outbox event, so a retried reading is stored once and never re-enters the alert pipeline. For conflicting rows, ingest compares `payload_hash`: same values = `DUPLICATE`, different values = `CONFLICT`. Redis `SET NX` is a fast pre-check only; the constraint is the guarantee.
2. **Outbox → Kafka**: each event gets a UUIDv7 `event_id` from the application at insert time. The relay publishes it (also as a Kafka header) with an idempotent producer and deletes the row after the broker ack. A crash between the two causes a re-publish with the same id, which step 3 absorbs.
3. **Evaluator**: in one transaction it locks the patient's unresolved episode for the measure (`FOR UPDATE`; `ux_alert_active_episode` is the backstop), opens a new alert or folds the breach in (`occurrence_cnt + 1`, severity upgrade), then inserts the `alert_occurrence` row. A primary-key conflict on the occurrence means the reading was already evaluated, so the whole transaction rolls back and nothing changes. New episodes and severity upgrades write an `alerts.raised` outbox row (topic `alerts.raised.critical` for CRITICAL) in the same transaction; escalations and coach actions write `alert.state_changed`. Kafka offsets are committed only after the DB commit. In production these steps run once per batch of up to 500 events; any unique conflict rolls the batch back, and it is replayed one event at a time.
4. **Notification**: unique `(trigger_event_id, coach_id, channel)`; an external send happens only when that row moves from `PENDING` to `SENT`. Every real change carries a new event id, so it notifies again; a replay reuses the id and does not.

### Race conditions and how each is handled

| Scenario | Risk | Mechanism |
| --- | --- | --- |
| Same batch retried and hitting two ingest pods at once | Duplicate rows, double alerts | Primary-key conflict; `RETURNING` tells each pod which rows it actually inserted |
| Retry reuses a reading id with different values | Silent data loss | `payload_hash` mismatch returns `CONFLICT` for that item and logs an integrity event |
| Two breaches for one patient and measure close together | Two alerts for one episode | Per-patient Kafka key keeps them on one consumer in order; `ux_alert_active_episode` covers every unresolved status as the backstop |
| HIGH breach then CRITICAL breach in one episode | Two parallel alerts, or the upgrade goes unnoticed | Severity upgraded in place; `SEVERITY_UPGRADED` notification |
| Kafka redelivers an evaluated event | Counter raised twice | `alert_occurrence` primary-key conflict rolls back the transaction |
| Out-of-order arrival (offline buffer flushed late) | Older value overwrites "latest" | Redis Lua compare-and-set only writes if `measured_at` is newer; DB reads order by `measured_at` |
| Device rebound while readings are still buffered | Old readings attributed to the new patient | Binding resolved at `measured_at` from binding history; `EXCLUDE` constraint prevents overlapping owners |
| Late reading (> 7 days old) | Stale alert pages a coach | Stored normally; alert flagged `is_late` and routed at low priority |
| Two coaches acknowledge the same alert | Lost update, wrong owner | Optimistic lock on `alert.version`; loser gets `409` and the current state |
| Threshold changed mid-evaluation | Inconsistent rule | Rules are immutable; the occurrence records the exact `threshold_id`; caches evicted on `thresholds.changed`, at most 10 min stale |
| Several outbox relays polling | Double publish | Single CDC connector in prod; locally `FOR UPDATE SKIP LOCKED LIMIT 500`; consumers dedupe on `event_id` |
| Escalation scheduler on many pods | Same alert escalated twice | `SELECT … FOR UPDATE SKIP LOCKED` + status guard in the `UPDATE`; escalate\_at cleared in the same update; notification keyed by event id, reason `ESCALATED` |
| Cache miss storm at 6 AM | DB overload on thresholds | Caffeine L1 with single-flight loading; jittered TTLs |
| Device clock wrong | Rows in wrong partition, bad ordering | Reject `measured_at` > now + 5 min or older than 90 days; keep `received_at`; DEFAULT partition for outliers |

### Backpressure

- Each ingest pod caps in-flight DB work with a bounded semaphore. When the DB pool wait exceeds 200 ms, or the outbox backlog exceeds a set row count, it returns `503` with `Retry-After` (jittered). Devices already buffer offline, so shedding is safe.
- Per-device token bucket in Redis returns `429` for misbehaving devices before they hurt others.
- Kafka consumers pause partitions when downstream writes slow, instead of buffering unbounded work in memory.

### Partial outages

| Component down | Ingest | Alerts | Reads |
| --- | --- | --- | --- |
| Kafka | Keeps working; outbox grows | Delayed until Kafka returns, then drained in order | Unaffected |
| Redis | Works; skips cache updates, auth falls back to DB with circuit breaker | Thresholds from DB (slower) | Latest readings from replicas |
| Postgres primary | `503` + `Retry-After`; devices buffer | Paused | Replicas serve stale reads |
| LLM provider | Unaffected | Unaffected; triage field left empty | Summary returns rule-based fallback |
| One consumer pod | Unaffected | Partitions rebalance (cooperative sticky), work resumes from last commit | Unaffected |

## Implementation roadmap

The build follows the request path: secure intake first, then idempotent storage, then the async pipeline, then load and AI. Each day ends at a gate that must pass before the next starts.

> Diagram: implementation roadmap · 4 build days, 4 gates, then production. See the [live design doc](https://claude.ai/artifact/CBKPwbpYbhHwWhCgCTvBhi) for the drawing.

The production phase reuses the same code; only the infrastructure changes (CDC instead of polling, managed Kafka, partition automation, replicas).

### Tests that matter most

- Retried batch, sent concurrently from two threads, yields one row and one alert (Testcontainers Postgres + Kafka).
- Kafka redelivery of the same event does not create a second alert or notification.
- Out-of-order readings never overwrite a newer latest-reading cache entry.
- Device bound to patient A cannot post for patient B (`403`).
- Concurrent acknowledge: one `200`, one `409`.
- LLM timeout returns the fallback summary within the latency budget.

### Migration from the monolith

1. **Shadow**: monolith forwards a copy of each reading to the new ingest endpoint; compare stored counts and alerts daily.
2. **Dual-read**: coach UI reads the alert queue from the new service behind a feature flag, per clinic.
3. **Cutover**: point device traffic at the gateway route for the new service, one device cohort at a time; monolith ingest stays as fallback for two weeks.
4. **Retire**: remove the monolith's ingest and alert code once error and lag SLOs hold for 14 days.

### Risks

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Devices without a stable reading UUID | Dedup breaks | Server derives a UUIDv5 from `(device_id, device_seq, measured_at, reading_type)`; firmware update as follow-up |
| Postgres write IOPS at the 6 AM spike | Ingest latency, `503`s | Batch inserts, pre-scaled storage, shard by `patient_id` when sustained writes pass \~70% of capacity |
| Hot patient or noisy device | One partition lags | Per-device rate limit; episode coalescing; lag alerts per partition |
| Alert fatigue | Coaches miss real criticals | Episode coalescing, `LATE` routing, severity ordering in the queue; confirmation rule, per-patient baselines, daily digests and a new-episodes-per-coach metric |
| LLM output wrong or leaks PHI | Clinical or compliance risk | Advisory only, de-identified prompts, BAA-covered provider, human review sampling |
