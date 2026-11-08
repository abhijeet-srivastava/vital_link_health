-- Alerts: one row per episode, and one alert_occurrence row per breaching reading folded into it.
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
-- No FK to reading on purpose: reading partitions are dropped, an FK would block that.
CREATE INDEX ix_occurrence_alert ON alert_occurrence (alert_id, measured_at);

-- Daily partitions like reading, dropped after 90 days; the episode keeps occurrence_cnt.
SET LOCAL timezone = 'UTC';
SELECT partman.create_parent(
  p_parent_table    := 'public.alert_occurrence',
  p_control         := 'measured_at',
  p_interval        := '1 day',
  p_premake         := 7,
  p_start_partition := to_char(current_date - 2, 'YYYY-MM-DD')
);
UPDATE partman.part_config
SET retention = '90 days',
    retention_keep_table = true,
    infinite_time_partitions = true
WHERE parent_table = 'public.alert_occurrence';
