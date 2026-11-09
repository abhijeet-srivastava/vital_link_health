-- Stage 10: hourly reading aggregates for the AI summary (security-phi.md, AI features; D18).
-- One row per patient, measure and UTC hour: count, min, max, mean. No device ids, reading ids, free text or extras;
-- the prompt builder drops patient_id and turns hour_start into "hours ago". Written by alert-worker's RollupJob, which
-- recomputes recent hours from reading (idempotent upsert on the primary key); read by query-api's summary endpoint.
--
-- Checklist: shape in references/schema.sql; the natural key includes the partition key (hour_start); no FK into
-- reading (its partitions are dropped); grants least privilege; RLS like reading (coach panel, D34); no new event type.
CREATE TABLE reading_rollup (
  patient_id     BIGINT       NOT NULL,
  measure_code   SMALLINT     NOT NULL REFERENCES measure,
  hour_start     TIMESTAMPTZ  NOT NULL,    -- UTC hour of measured_at; partition key
  n              INT          NOT NULL CHECK (n >= 1),
  min_value      NUMERIC(8,2) NOT NULL,
  max_value      NUMERIC(8,2) NOT NULL,
  avg_value      NUMERIC(8,2) NOT NULL,
  updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (patient_id, measure_code, hour_start),   -- rollup idempotency key; also serves the per-patient read
  CHECK (min_value <= max_value),
  CHECK (hour_start = date_trunc('hour', hour_start, 'UTC'))
) PARTITION BY RANGE (hour_start);

SET LOCAL timezone = 'UTC';
SELECT partman.create_parent(
  p_parent_table    := 'public.reading_rollup',
  p_control         := 'hour_start',
  p_interval        := '1 day',
  p_premake         := 7,
  p_start_partition := to_char(current_date - 2, 'YYYY-MM-DD')
);
UPDATE partman.part_config
SET retention = '90 days',
    retention_keep_table = false,          -- derived from reading: dropped, never archived
    infinite_time_partitions = true
WHERE parent_table = 'public.reading_rollup';

-- RollupJob (vitallink_service) upserts; query-api coach pools (vitallink_query) read under the panel policy.
GRANT SELECT, INSERT, UPDATE ON reading_rollup TO vitallink_service;
GRANT SELECT ON reading_rollup TO vitallink_query;

ALTER TABLE reading_rollup ENABLE ROW LEVEL SECURITY;
CREATE POLICY service_all ON reading_rollup TO vitallink_service USING (true) WITH CHECK (true);
CREATE POLICY coach_panel ON reading_rollup TO vitallink_query
  USING (patient_id IN (
    SELECT ca.patient_id FROM care_assignment ca
    WHERE ca.coach_id = NULLIF(current_setting('app.coach_id', true), '')::bigint
      AND ca.valid_to IS NULL AND ca.valid_from <= now()));
