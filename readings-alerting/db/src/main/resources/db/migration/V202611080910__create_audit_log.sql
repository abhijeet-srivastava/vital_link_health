-- Audit: append-only PHI access trail. The service role gets INSERT only (V202611080912);
-- partitions are exported daily to WORM storage.
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

SET LOCAL timezone = 'UTC';
SELECT partman.create_parent(
  p_parent_table    := 'public.audit_log',
  p_control         := 'at',
  p_interval        := '1 day',
  p_premake         := 7,
  p_start_partition := to_char(current_date - 2, 'YYYY-MM-DD')
);
UPDATE partman.part_config
SET retention = '90 days',
    retention_keep_table = true,           -- detached, never dropped here: the WORM export owns deletion
    infinite_time_partitions = true
WHERE parent_table = 'public.audit_log';
