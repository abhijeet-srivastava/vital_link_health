-- Threshold rules: immutable, one band per row. A change closes the row (valid_to) and inserts a new one.
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
