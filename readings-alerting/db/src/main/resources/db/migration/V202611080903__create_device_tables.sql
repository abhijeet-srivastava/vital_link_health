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
