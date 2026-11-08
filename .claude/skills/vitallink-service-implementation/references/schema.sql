-- VitalLink Readings & Alerting - canonical schema (PostgreSQL 16)
-- Source of truth for table shapes, keys and constraints. Migrations must converge to this.
-- Every unique key below is an idempotency or business-rule guard; do not drop or weaken one
-- without updating references/idempotency-and-concurrency.md and references/decisions.md.

CREATE EXTENSION IF NOT EXISTS btree_gist;   -- for the no-overlap EXCLUDE constraints

-- ===== Reference data (small, cached) =====
CREATE TABLE measure (                     -- one row per measurable quantity
  measure_code   SMALLINT PRIMARY KEY,     -- 1 SYSTOLIC, 2 DIASTOLIC, 3 PULSE, 4 GLUCOSE, 5 SPO2, 6 WEIGHT
  name           TEXT NOT NULL UNIQUE,
  reading_type   SMALLINT NOT NULL,        -- 1 BLOOD_PRESSURE, 2 GLUCOSE, 3 PULSE_OX, 4 WEIGHT: the measure's usual
                                           -- device type only; split readings with reading_type_measure (D22)
  unit           TEXT NOT NULL,            -- canonical unit; ingest converts (mmol/L -> mg/dL)
  min_plausible  NUMERIC(8,2) NOT NULL,    -- device-error guard, e.g. systolic 40
  max_plausible  NUMERIC(8,2) NOT NULL,    -- e.g. systolic 300
  CHECK (min_plausible < max_plausible)
);

CREATE TABLE reading_type_measure (        -- which measures a reading type carries; a measure may come from several (D22)
  reading_type   SMALLINT NOT NULL,        -- 1 BLOOD_PRESSURE, 2 GLUCOSE, 3 PULSE_OX, 4 WEIGHT
  measure_code   SMALLINT NOT NULL REFERENCES measure,
  PRIMARY KEY (reading_type, measure_code)
);
-- Rows: (1: SYSTOLIC, DIASTOLIC, PULSE) (2: GLUCOSE) (3: SPO2, PULSE) (4: WEIGHT). PULSE is read from reading.pulse
-- for both BP and pulse-ox rows.

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
-- Binding at a past time: WHERE device_id = $1 AND valid_from <= $t ORDER BY valid_from DESC LIMIT 1 (uses the PK)

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
  PRIMARY KEY (patient_id, coach_id, role, valid_from),
  CONSTRAINT ex_primary_coach_no_overlap   -- one PRIMARY at any instant, history included; handover = touching ranges
    EXCLUDE USING gist (patient_id WITH =, tstzrange(valid_from, valid_to) WITH &&) WHERE (role = 'PRIMARY')
);
CREATE UNIQUE INDEX ux_primary_coach ON care_assignment (patient_id) INCLUDE (coach_id)
  WHERE role = 'PRIMARY' AND valid_to IS NULL;
CREATE INDEX ix_coach_panel ON care_assignment (coach_id, patient_id) WHERE valid_to IS NULL;

-- ===== Threshold rules: immutable, one band per row =====
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
  systolic       SMALLINT, diastolic SMALLINT, pulse SMALLINT,   -- one measurement, several values: one row
  value_num      NUMERIC(8,2),            -- glucose, SpO2, weight in the canonical unit
  payload_hash   BIGINT      NOT NULL,    -- 64-bit hash of the values: same id + different values = conflict
  extras         JSONB,                   -- rare vendor fields only
  PRIMARY KEY (device_id, reading_uuid, measured_at),
  CHECK (reading_type <> 1 OR (systolic IS NOT NULL AND diastolic IS NOT NULL)),
  CHECK (reading_type = 1 OR value_num IS NOT NULL)
) PARTITION BY RANGE (measured_at);
-- Daily partitions created 7 days ahead by pg_partman; DEFAULT partition catches clock-skewed rows
-- (alert if it is non-empty). After 90 days: detach -> Parquet on S3 (Object Lock) -> drop.
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
CREATE INDEX ix_alert_queue ON alert (coach_id, severity_rank, created_at, alert_id)  -- alert_id: keyset tie-breaker (D21)
  WHERE status <> 'RESOLVED';
CREATE INDEX ix_alert_escalation ON alert (escalate_at) WHERE escalate_at IS NOT NULL;

CREATE TABLE alert_occurrence (            -- each breaching reading folded into an episode (~10M rows/day at 20%)
  device_id       BIGINT NOT NULL,
  reading_uuid    UUID NOT NULL,
  measure_code    SMALLINT NOT NULL,
  measured_at     TIMESTAMPTZ NOT NULL,    -- partition key; with device_id + reading_uuid = the reading's key
  alert_id        BIGINT NOT NULL REFERENCES alert,
  observed_value  NUMERIC(8,2) NOT NULL,
  threshold_id    BIGINT NOT NULL REFERENCES threshold_rule,  -- the exact rule version that fired
  severity        TEXT NOT NULL CHECK (severity IN ('CRITICAL','HIGH','MEDIUM')),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (device_id, reading_uuid, measure_code, measured_at)  -- alert idempotency key; a replay has the same measured_at
) PARTITION BY RANGE (measured_at);
-- Daily partitions by pg_partman, dropped after 90 days like reading; the episode keeps occurrence_cnt.
-- No FK to reading on purpose: reading partitions are dropped, an FK would block that.
CREATE INDEX ix_occurrence_alert ON alert_occurrence (alert_id, measured_at);

CREATE TABLE notification (
  notification_id  BIGSERIAL PRIMARY KEY,
  trigger_event_id UUID NOT NULL,          -- the alerts.raised / alert.state_changed event that caused it
  alert_id         BIGINT NOT NULL REFERENCES alert,
  coach_id         BIGINT NOT NULL,
  channel          TEXT NOT NULL CHECK (channel IN ('QUEUE','PUSH','SMS','DIGEST')),
  reason           TEXT NOT NULL CHECK (reason IN ('RAISED','SEVERITY_UPGRADED','ESCALATED','REASSIGNED')),
  severity         TEXT NOT NULL CHECK (severity IN ('CRITICAL','HIGH','MEDIUM')),  -- severity at the time; shown in the message
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
-- Ingest DB:   device, device_credential, device_binding, measure, reading_type_measure, reading, outbox_event, audit_log
-- Alerting DB: measure, reading_type_measure, threshold_rule, enrollment, care_assignment, notification_policy, alert,
--              alert_occurrence, notification, outbox_event, processed_event, audit_log
-- Both later sharded by patient_id (device tables by device_id) so a patient's rows stay on one shard.

-- ===== Access: row-level security (V202611100900; role-dependent, so documented here, not applied by the
-- conformance test) =====
-- Role vitallink_query (query-api coach pools): SELECT reading, alert, care_assignment, measure, threshold_rule,
-- enrollment; UPDATE alert (status, escalate_at, version, acknowledged_*, resolved_*, resolution_note);
-- INSERT + UPDATE (valid_to) threshold_rule; INSERT outbox_event, audit_log.
-- ALTER TABLE reading ENABLE ROW LEVEL SECURITY;  ALTER TABLE alert ENABLE ROW LEVEL SECURITY;
-- POLICY service_all ON reading|alert TO vitallink_service USING (true) WITH CHECK (true);
-- POLICY coach_panel ON reading|alert TO vitallink_query
--   USING (patient_id IN (SELECT patient_id FROM care_assignment
--          WHERE coach_id = NULLIF(current_setting('app.coach_id', true), '')::bigint
--            AND valid_to IS NULL AND valid_from <= now()))      -- same WITH CHECK on alert
-- FUNCTION alert_patient_id(bigint) RETURNS bigint SECURITY DEFINER  -- EXECUTE to vitallink_query only
