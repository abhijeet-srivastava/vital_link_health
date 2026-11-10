-- Reference data (small, cached). Rows are inserted by the next migration: every environment needs them.
CREATE TABLE measure (                     -- one row per measurable quantity
  measure_code   SMALLINT PRIMARY KEY,     -- 1 SYSTOLIC, 2 DIASTOLIC, 3 PULSE, 4 GLUCOSE, 5 SPO2, 6 WEIGHT
  name           TEXT NOT NULL UNIQUE,
  reading_type   SMALLINT NOT NULL,        -- 1 BLOOD_PRESSURE, 2 GLUCOSE, 3 PULSE_OX, 4 WEIGHT
  unit           TEXT NOT NULL,            -- canonical unit; ingest converts (mmol/L -> mg/dL)
  min_plausible  NUMERIC(8,2) NOT NULL,    -- device-error guard, e.g. systolic 40
  max_plausible  NUMERIC(8,2) NOT NULL,    -- e.g. systolic 300
  CHECK (min_plausible < max_plausible)
);
