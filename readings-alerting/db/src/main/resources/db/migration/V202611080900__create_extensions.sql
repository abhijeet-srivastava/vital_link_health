-- btree_gist: the no-overlap EXCLUDE constraints (device_binding, enrollment).
-- pg_partman: daily partitions of reading, alert_occurrence and audit_log, in its own schema.
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
