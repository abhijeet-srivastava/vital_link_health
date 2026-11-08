-- Projections owned by other services (kept in sync from their events).
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
