-- Hot table: ~50M rows/day. Daily partitions (UTC days) by pg_partman from today-2 to today+7,
-- plus reading_default for clock-skewed rows (alert if it is non-empty).
-- After 90 days: detach -> Parquet on S3 (Object Lock) -> drop.
CREATE TABLE reading (
  device_id      BIGINT      NOT NULL,
  reading_uuid   UUID        NOT NULL,    -- from the device; if missing, server derives UUIDv5(device_id, device_seq, measured_at, reading_type)
  measured_at    TIMESTAMPTZ NOT NULL,    -- device clock; partition key
  received_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  patient_id     BIGINT      NOT NULL,    -- owner at measured_at (binding history), not owner now
  reading_type   SMALLINT    NOT NULL,
  device_seq     BIGINT,
  systolic       SMALLINT, diastolic SMALLINT, pulse SMALLINT,   -- one measurement, several values: one row
  value_num      NUMERIC(8,2),            -- glucose, SpO2, weight in the canonical unit
  payload_hash   BIGINT      NOT NULL,    -- 64-bit hash of the values: same id + different values = conflict
  extras         JSONB,                   -- rare vendor fields only
  PRIMARY KEY (device_id, reading_uuid, measured_at),
  CHECK (reading_type <> 1 OR (systolic IS NOT NULL AND diastolic IS NOT NULL)),
  CHECK (reading_type = 1 OR value_num IS NOT NULL)
) PARTITION BY RANGE (measured_at);
CREATE INDEX ix_reading_patient_time ON reading (patient_id, reading_type, measured_at DESC, reading_uuid);

-- Partition boundaries follow the session time zone: pin UTC for this transaction.
SET LOCAL timezone = 'UTC';
SELECT partman.create_parent(
  p_parent_table    := 'public.reading',
  p_control         := 'measured_at',
  p_interval        := '1 day',
  p_premake         := 7,
  p_start_partition := to_char(current_date - 2, 'YYYY-MM-DD')
);
UPDATE partman.part_config
SET retention = '90 days',
    retention_keep_table = true,           -- keep the detached table for the archival job
    infinite_time_partitions = true        -- keep making future days even when no new rows arrive
WHERE parent_table = 'public.reading';
