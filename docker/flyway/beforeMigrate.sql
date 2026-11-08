-- Compose only (Flyway callback, runs before every migrate as the owner role; not a migration, not checksummed).
-- Creates or updates the least-privilege login user the apps connect as. Its privileges come from membership in
-- vitallink_service (V202611080912__create_roles_and_grants.sql); the password comes from .env.
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vitallink_app') THEN
    CREATE ROLE vitallink_app LOGIN;
  END IF;
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'vitallink_service') THEN
    GRANT vitallink_service TO vitallink_app;
  END IF;
END $$;
ALTER ROLE vitallink_app WITH LOGIN PASSWORD '${app_db_password}';
