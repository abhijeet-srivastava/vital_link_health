-- Least-privilege role for the three services. Migrations and partition DDL run as the owner role.
-- vitallink_service is NOLOGIN; the login user (vitallink_app, created outside migrations with a password
-- from the environment) is made a member when it exists. Later migrations grant on the tables they add.
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vitallink_service') THEN
    CREATE ROLE vitallink_service NOLOGIN;
  END IF;
END $$;

GRANT USAGE ON SCHEMA public TO vitallink_service;

-- Reference data and projections: read and maintain.
GRANT SELECT ON measure TO vitallink_service;
GRANT SELECT, INSERT, UPDATE ON device, device_credential, device_binding,
                               enrollment, care_assignment, notification_policy TO vitallink_service;
-- Threshold rules are immutable: a change closes a row (UPDATE valid_to) and inserts a new one.
GRANT SELECT, INSERT, UPDATE ON threshold_rule TO vitallink_service;

-- Time-series and event tables.
GRANT SELECT, INSERT ON reading, alert_occurrence TO vitallink_service;
GRANT SELECT, INSERT, DELETE ON outbox_event, processed_event TO vitallink_service;  -- relay deletes, purge job deletes
GRANT SELECT, INSERT, UPDATE ON alert, notification TO vitallink_service;

-- Audit is append-only for the service: no SELECT, UPDATE, DELETE or TRUNCATE.
GRANT INSERT ON audit_log TO vitallink_service;

GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO vitallink_service;

GRANT USAGE ON SCHEMA partman_maint TO vitallink_service;
GRANT EXECUTE ON FUNCTION partman_maint.run() TO vitallink_service;

DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'vitallink_app') THEN
    GRANT vitallink_service TO vitallink_app;
  END IF;
END $$;
