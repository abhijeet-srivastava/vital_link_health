-- Dev seed: compose and tests only (Flyway location db/seed; never on a real environment's path).
-- Synthetic IDs only, no names or contact details. Fixed timestamps (not now()) so the script is repeatable;
-- every insert is ON CONFLICT DO NOTHING, so rerunning it changes nothing.
-- Device API keys are NOT seeded here: secrets must not live in the repo. The ingest-api `seed` profile
-- mints them (random secret, HMAC with the pepper from the environment) and logs five demo keys.
--
--   Programs     1 Hypertension, 2 Diabetes. Bands are illustrative, not clinically signed off: every row has
--                created_by = 'SEED-NOT-CLINICAL', and the apps refuse to start with the prod profile while one exists.
--   Devices      1-600 BP_CUFF, 601-850 GLUCOMETER, 851-1000 PULSE_OX; device i -> patient i.
--                Device 1 was bound to patient 1000 until 2026-10-01 (the rebinding case).
--   Enrollment   patients 1-700 -> program 1, 701-1000 -> program 2, patient 1 in both (multi-program case).
--   Coaches      1-20, 50 patients each: PRIMARY ((p-1) % 20) + 1, BACKUP (p % 20) + 1.

-- ===== Program default threshold bands (one band per row) =====
INSERT INTO threshold_rule (program_id, measure_code, operator, limit_value, severity, valid_from, created_by)
SELECT b.program_id, b.measure_code, b.operator, b.limit_value, b.severity, '2026-01-01T00:00Z', 'SEED-NOT-CLINICAL'
FROM (VALUES
  -- program 1: hypertension
  (1, 1, 'GT', 160, 'HIGH'), (1, 1, 'GT', 180, 'CRITICAL'), (1, 1, 'LT',  90, 'CRITICAL'),   -- systolic mmHg
  (1, 2, 'GT', 100, 'HIGH'), (1, 2, 'GT', 120, 'CRITICAL'), (1, 2, 'LT',  50, 'HIGH'),       -- diastolic mmHg
  (1, 3, 'GT', 120, 'HIGH'), (1, 3, 'GT', 150, 'CRITICAL'), (1, 3, 'LT',  40, 'CRITICAL'),   -- pulse bpm
  (1, 4, 'GT', 250, 'HIGH'), (1, 4, 'GT', 400, 'CRITICAL'), (1, 4, 'LT',  54, 'CRITICAL'),   -- glucose mg/dL
  (1, 5, 'LT',  92, 'HIGH'), (1, 5, 'LT',  88, 'CRITICAL'),                                  -- SpO2 %
  -- program 2: diabetes (tighter glucose bands)
  (2, 1, 'GT', 160, 'HIGH'), (2, 1, 'GT', 180, 'CRITICAL'), (2, 1, 'LT',  90, 'CRITICAL'),
  (2, 2, 'GT', 100, 'HIGH'), (2, 2, 'GT', 120, 'CRITICAL'), (2, 2, 'LT',  50, 'HIGH'),
  (2, 3, 'GT', 120, 'HIGH'), (2, 3, 'GT', 150, 'CRITICAL'), (2, 3, 'LT',  40, 'CRITICAL'),
  (2, 4, 'GT', 180, 'MEDIUM'), (2, 4, 'GT', 250, 'HIGH'), (2, 4, 'GT', 400, 'CRITICAL'),
  (2, 4, 'LT',  70, 'HIGH'), (2, 4, 'LT',  54, 'CRITICAL'),
  (2, 5, 'LT',  92, 'HIGH'), (2, 5, 'LT',  88, 'CRITICAL')
) AS b (program_id, measure_code, operator, limit_value, severity)
ON CONFLICT (program_id, measure_code, operator, severity) WHERE program_id IS NOT NULL AND valid_to IS NULL
DO NOTHING;

-- Relabel rows from an earlier version of this seed (dev volumes only).
UPDATE threshold_rule SET created_by = 'SEED-NOT-CLINICAL' WHERE created_by = 'dev-seed';

-- ===== Devices and bindings =====
INSERT INTO device (device_id, serial_no, device_type, created_at)
SELECT d, format('DEV-%s', lpad(d::text, 6, '0')),
       CASE WHEN d <= 600 THEN 'BP_CUFF' WHEN d <= 850 THEN 'GLUCOMETER' ELSE 'PULSE_OX' END,
       '2026-01-01T00:00Z'
FROM generate_series(1, 1000) AS d
ON CONFLICT DO NOTHING;

INSERT INTO device_binding (device_id, patient_id, valid_from, valid_to) VALUES
  (1, 1000, '2026-01-01T00:00Z', '2026-10-01T00:00Z')
ON CONFLICT DO NOTHING;                    -- no target: also skips rows the EXCLUDE constraint would reject

INSERT INTO device_binding (device_id, patient_id, valid_from)
SELECT d, d, CASE WHEN d = 1 THEN timestamptz '2026-10-01T00:00Z' ELSE timestamptz '2026-01-01T00:00Z' END
FROM generate_series(1, 1000) AS d
ON CONFLICT DO NOTHING;

-- ===== Enrollment (projection) =====
INSERT INTO enrollment (patient_id, program_id, valid_from)
SELECT p, CASE WHEN p <= 700 THEN 1 ELSE 2 END, timestamptz '2026-01-01T00:00Z'
FROM generate_series(1, 1000) AS p
UNION ALL
SELECT 1, 2, timestamptz '2026-01-01T00:00Z'
ON CONFLICT DO NOTHING;

-- ===== Care assignments (projection) =====
INSERT INTO care_assignment (patient_id, coach_id, role, valid_from)
SELECT p, ((p - 1) % 20) + 1, 'PRIMARY', timestamptz '2026-01-01T00:00Z' FROM generate_series(1, 1000) AS p
UNION ALL
SELECT p, (p % 20) + 1, 'BACKUP', timestamptz '2026-01-01T00:00Z' FROM generate_series(1, 1000) AS p
ON CONFLICT DO NOTHING;
