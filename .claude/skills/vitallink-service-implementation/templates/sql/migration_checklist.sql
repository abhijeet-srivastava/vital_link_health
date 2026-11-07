-- Flyway migration template: V<yyyymmddhhmm>__<verb>_<object>.sql
-- Copy, then delete the checklist comments that do not apply. Never edit an applied migration.
--
-- Checklist
-- [ ] Shape matches references/schema.sql (update that file in the same PR).
-- [ ] Every retried write has a natural UNIQUE key; partitioned tables include the partition key in it.
-- [ ] Enum-like TEXT columns have a CHECK constraint; cross-column rules are CHECKs, not app code.
-- [ ] Time columns are TIMESTAMPTZ; measured_at (device) and received_at (server) stay separate.
-- [ ] Hot-row counters / timestamps updated on every event are NOT indexed (keeps updates HOT);
--     consider WITH (fillfactor = 80) on frequently updated tables.
-- [ ] Partial indexes for "active" subsets (e.g. WHERE status <> 'RESOLVED').
-- [ ] Index supports the keyset pagination order, with a unique tie-breaker as the last column.
-- [ ] No FK into a partitioned time-series table whose partitions are dropped (store the full key).
-- [ ] Large tables: CREATE INDEX CONCURRENTLY in its own non-transactional migration.
-- [ ] Adding NOT NULL to an existing big table: add nullable -> backfill in batches -> validate.
-- [ ] Grants: service role least privilege; audit tables INSERT only.
-- [ ] New event type? Add it to outbox_event.event_type CHECK and document the payload.

-- Example: new partitioned time-series table
CREATE TABLE example_event (
  device_id     BIGINT      NOT NULL,
  event_uuid    UUID        NOT NULL,              -- idempotency key from the producer
  occurred_at   TIMESTAMPTZ NOT NULL,              -- partition key
  received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  patient_id    BIGINT      NOT NULL,
  kind          TEXT        NOT NULL CHECK (kind IN ('A','B')),
  PRIMARY KEY (device_id, event_uuid, occurred_at)
) PARTITION BY RANGE (occurred_at);

CREATE INDEX ix_example_event_patient_time
  ON example_event (patient_id, occurred_at DESC, event_uuid);

-- pg_partman: daily partitions, 7 ahead, 90-day retention (detach, archive, drop)
SELECT partman.create_parent(
  p_parent_table := 'public.example_event',
  p_control      := 'occurred_at',
  p_interval     := '1 day',
  p_premake      := 7
);
UPDATE partman.part_config
SET retention = '90 days', retention_keep_table = true   -- keep detached table for archival job
WHERE parent_table = 'public.example_event';
