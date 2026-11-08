-- Stage 4: the local outbox relay claims rows with SELECT ... FOR UPDATE SKIP LOCKED (pattern 7).
-- Row locks need UPDATE privilege on at least one column; vitallink_service had only SELECT, INSERT, DELETE.
-- A single-column grant is enough for the lock and keeps the service from rewriting topics, keys or payloads.
--
-- Checklist: no shape change (schema.sql has no grants); grants stay least privilege; no new event type.
GRANT UPDATE (created_at) ON outbox_event TO vitallink_service;
