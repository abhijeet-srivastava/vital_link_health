-- Codes are fixed (schema.sql comment, domain enums). Plausibility bounds reject device errors at ingest,
-- they are not clinical thresholds (those live in threshold_rule).
INSERT INTO measure (measure_code, name, reading_type, unit, min_plausible, max_plausible) VALUES
  (1, 'SYSTOLIC',  1, 'mmHg',  40,  300),
  (2, 'DIASTOLIC', 1, 'mmHg',  20,  200),
  (3, 'PULSE',     1, 'bpm',   20,  250),
  (4, 'GLUCOSE',   2, 'mg/dL', 10, 1000),
  (5, 'SPO2',      3, '%',     50,  100),
  (6, 'WEIGHT',    4, 'kg',     2,  500);
