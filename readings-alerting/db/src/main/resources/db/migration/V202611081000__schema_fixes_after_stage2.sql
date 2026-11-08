-- Schema fixes found in the stage 2 review (references/schema.sql updated in the same change; decisions D21, D22).
-- Runs after V202611080902__insert_measures.sql (reference rows) and V202611080912 (roles). All tables are still
-- empty in every environment, so plain DDL inside the migration transaction is fine; on a populated alert table the
-- index swap would need CREATE INDEX CONCURRENTLY in its own non-transactional migration.

-- 1, 2. Severity is a closed set everywhere it is stored, not only on alert and threshold_rule.
ALTER TABLE alert_occurrence ADD CONSTRAINT alert_occurrence_severity_check
  CHECK (severity IN ('CRITICAL','HIGH','MEDIUM'));        -- partitioned: applies to every partition
ALTER TABLE notification ADD CONSTRAINT notification_severity_check
  CHECK (severity IN ('CRITICAL','HIGH','MEDIUM'));

-- 3. At most one PRIMARY coach per patient at any instant, history included. ux_primary_coach covers only the
--    current row; this also rejects overlapping closed rows. A handover (old valid_to = new valid_from) is not an
--    overlap ([) ranges), and BACKUP rows are unconstrained.
ALTER TABLE care_assignment ADD CONSTRAINT ex_primary_coach_no_overlap
  EXCLUDE USING gist (patient_id WITH =, tstzrange(valid_from, valid_to) WITH &&) WHERE (role = 'PRIMARY');

-- 4. Which measures a reading type carries. measure.reading_type allows one type per measure, but PULSE comes from
--    a BP cuff (reading.pulse) and from a pulse oximeter (reading.pulse alongside SpO2 in value_num).
CREATE TABLE reading_type_measure (
  reading_type   SMALLINT NOT NULL,        -- 1 BLOOD_PRESSURE, 2 GLUCOSE, 3 PULSE_OX, 4 WEIGHT
  measure_code   SMALLINT NOT NULL REFERENCES measure,
  PRIMARY KEY (reading_type, measure_code)
);
INSERT INTO reading_type_measure (reading_type, measure_code) VALUES
  (1, 1), (1, 2), (1, 3),                  -- BP cuff: systolic, diastolic, pulse
  (2, 4),                                  -- glucometer: glucose
  (3, 5), (3, 3),                          -- pulse oximeter: SpO2, pulse
  (4, 6);                                  -- scale: weight
GRANT SELECT ON reading_type_measure TO vitallink_service;

-- 5. Coach queue keyset cursor (severity_rank, created_at, alert_id): alert_id is the unique tie-breaker, so two
--    alerts created in the same microsecond can be neither skipped nor repeated across pages.
DROP INDEX ix_alert_queue;
CREATE INDEX ix_alert_queue ON alert (coach_id, severity_rank, created_at, alert_id) WHERE status <> 'RESOLVED';
