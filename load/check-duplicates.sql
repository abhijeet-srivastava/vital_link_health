-- Extra rows from retries, over the whole database; must be 0.
-- reading: the device's reading_uuid is the identity (the primary key also carries measured_at, the partition key,
-- so a key check alone would not catch the same reading stored on two days). alert_occurrence: one per reading
-- and measure.
SELECT (SELECT count(*) - count(DISTINCT (device_id, reading_uuid)) FROM reading)
     + (SELECT count(*) - count(DISTINCT (device_id, reading_uuid, measure_code)) FROM alert_occurrence)
       AS extra_rows;
