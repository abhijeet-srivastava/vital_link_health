-- Stage 8: coach access enforced a second time by the database (security-phi.md, rule 9).
-- query-api connects as a member of vitallink_query and sets app.coach_id per transaction
-- (SELECT set_config('app.coach_id', '<id>', true)). Row-level security then shows that role only the rows of patients
-- in the coach's current care_assignment, whatever the SQL asks for. ingest-api, alert-worker and query-api's internal
-- admin endpoint keep using vitallink_service, which gets a permissive policy, so their behaviour does not change.
--
-- Checklist: no table shape change (schema.sql documents the access section); grants least privilege (column-level
-- UPDATE, audit_log INSERT only); no new event type (thresholds.changed is already in the CHECK).

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vitallink_query') THEN
    CREATE ROLE vitallink_query NOLOGIN;
  END IF;
END $$;

GRANT USAGE ON SCHEMA public TO vitallink_query;

-- Reads: readings and alerts are filtered by the policies below; care_assignment is read by the policies themselves
-- (policy subqueries run with the caller's privileges) and by the controller's panel check.
GRANT SELECT ON measure, care_assignment, reading, alert, threshold_rule, enrollment TO vitallink_query;

-- Coach actions: only the columns acknowledge/resolve change.
GRANT UPDATE (status, escalate_at, version, acknowledged_by, acknowledged_at, resolved_by, resolved_at, resolution_note)
  ON alert TO vitallink_query;

-- Threshold changes: close a row (valid_to) and insert new ones; rules are otherwise immutable.
GRANT INSERT, UPDATE (valid_to) ON threshold_rule TO vitallink_query;
GRANT USAGE ON SEQUENCE threshold_rule_threshold_id_seq TO vitallink_query;

-- Events and audit: append only.
GRANT INSERT ON outbox_event, audit_log TO vitallink_query;

ALTER TABLE reading ENABLE ROW LEVEL SECURITY;
ALTER TABLE alert ENABLE ROW LEVEL SECURITY;

CREATE POLICY service_all ON reading TO vitallink_service USING (true) WITH CHECK (true);
CREATE POLICY service_all ON alert TO vitallink_service USING (true) WITH CHECK (true);

-- NULLIF: once a SET LOCAL has ended, a pooled session reports '' rather than NULL; '' must mean "no coach", which
-- matches no rows, instead of failing the cast.
CREATE POLICY coach_panel ON reading TO vitallink_query
  USING (patient_id IN (
    SELECT ca.patient_id FROM care_assignment ca
    WHERE ca.coach_id = NULLIF(current_setting('app.coach_id', true), '')::bigint
      AND ca.valid_to IS NULL AND ca.valid_from <= now()));

CREATE POLICY coach_panel ON alert TO vitallink_query
  USING (patient_id IN (
    SELECT ca.patient_id FROM care_assignment ca
    WHERE ca.coach_id = NULLIF(current_setting('app.coach_id', true), '')::bigint
      AND ca.valid_to IS NULL AND ca.valid_from <= now()))
  WITH CHECK (patient_id IN (
    SELECT ca.patient_id FROM care_assignment ca
    WHERE ca.coach_id = NULLIF(current_setting('app.coach_id', true), '')::bigint
      AND ca.valid_to IS NULL AND ca.valid_from <= now()));

-- The controller must know whose alert a coach tried to act on, to deny and audit with the patient id, although the
-- policy hides the row. This returns the patient id only, runs as the owner (which RLS does not apply to) and is
-- executable by vitallink_query alone.
CREATE FUNCTION alert_patient_id(p_alert_id bigint) RETURNS bigint
  LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public
  AS $$ SELECT patient_id FROM alert WHERE alert_id = p_alert_id $$;
REVOKE ALL ON FUNCTION alert_patient_id(bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION alert_patient_id(bigint) TO vitallink_query;

DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'vitallink_query_app') THEN
    GRANT vitallink_query TO vitallink_query_app;
  END IF;
END $$;
