-- Post-run numbers for one load run, from the database (no metrics stack in this cut).
-- psql -v t0=<k6 start> -v t1=<k6 end> -v t2=<pipeline drained>; all DB clock.
-- Timestamps are transaction start times (now()): reading.received_at is when the ingest transaction began,
-- alert_occurrence.created_at when the evaluator transaction began. Latencies below therefore exclude the
-- evaluator's own commit time (bounded by its 200 ms batch window plus the commit).
\pset footer off
\set QUIET on

\echo '== ingest (rows stored between k6 start and end)'
WITH r AS (SELECT received_at FROM reading WHERE received_at BETWEEN :'t0' AND :'t1'),
     per_s AS (SELECT date_trunc('second', received_at) s, count(*) n FROM r GROUP BY 1)
SELECT (SELECT count(*) FROM r)                                                        AS rows_stored,
       round((SELECT count(*) FROM r) / extract(epoch FROM (:'t1'::timestamptz - :'t0'::timestamptz)))
                                                                                         AS avg_rows_per_s,
       (SELECT max(n) FROM per_s)                                                        AS peak_rows_per_s,
       (SELECT round(avg(n)) FROM per_s WHERE n >= (SELECT 0.9 * max(n) FROM per_s))     AS top10pct_seconds_avg;

\echo '== duplicates in this run (extra rows for the same reading_uuid)'
SELECT count(*) - count(DISTINCT (device_id, reading_uuid)) AS extra_reading_rows
FROM reading WHERE received_at BETWEEN :'t0' AND :'t1';

\echo '== evaluator (occurrence-writing transactions between k6 start and drain)'
WITH o AS (SELECT created_at FROM alert_occurrence WHERE created_at BETWEEN :'t0' AND :'t2'),
     tx AS (SELECT created_at, count(*) n FROM o GROUP BY created_at),
     per_s AS (SELECT date_trunc('second', created_at) s, count(*) c FROM tx GROUP BY 1)
SELECT (SELECT count(*) FROM o)                                              AS occurrences,
       (SELECT count(*) FROM tx)                                             AS commits,
       round((SELECT count(*) FROM tx)
             / greatest(extract(epoch FROM (SELECT max(created_at) - min(created_at) FROM o)), 1), 1)
                                                                               AS avg_commits_per_s,
       (SELECT max(c) FROM per_s)                                            AS peak_commits_per_s,
       (SELECT round(avg(n), 1) FROM tx)                                     AS occurrences_per_commit,
       (SELECT max(n) FROM tx)                                               AS max_occurrences_per_commit;

\echo '== reading received -> evaluator transaction (ms), by occurrence severity'
SELECT o.severity,
       count(*)                                                                                          AS n,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM o.created_at - r.received_at)) * 1000) AS p50_ms,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM o.created_at - r.received_at)) * 1000) AS p95_ms,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM o.created_at - r.received_at)) * 1000) AS p99_ms,
       round(max(extract(epoch FROM o.created_at - r.received_at)) * 1000)                               AS max_ms
FROM alert_occurrence o
JOIN reading r USING (device_id, reading_uuid, measured_at)
WHERE r.received_at BETWEEN :'t0' AND :'t1'
GROUP BY o.severity ORDER BY o.severity;

\echo '== CRITICAL episodes opened or upgraded in this run: first critical reading -> QUEUE notification row (ms); the alert itself is in the queue at the evaluator transaction above'
WITH first_crit AS (
  SELECT o.alert_id, min(r.received_at) AS received_at
  FROM alert_occurrence o JOIN reading r USING (device_id, reading_uuid, measured_at)
  WHERE o.severity = 'CRITICAL' AND r.received_at BETWEEN :'t0' AND :'t1'
  GROUP BY o.alert_id),
q AS (
  SELECT f.alert_id, extract(epoch FROM min(n.created_at) - f.received_at) AS secs
  FROM first_crit f JOIN notification n ON n.alert_id = f.alert_id
   AND n.channel = 'QUEUE' AND n.severity = 'CRITICAL' AND n.created_at >= f.received_at
  GROUP BY f.alert_id, f.received_at)
SELECT (SELECT count(*) FROM first_crit)                                              AS alerts_with_critical_readings,
       (SELECT count(*) FROM q)                                                       AS raised_or_upgraded_to_critical,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY secs) * 1000)               AS notif_p50_ms,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY secs) * 1000)               AS notif_p95_ms,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY secs) * 1000)               AS notif_p99_ms
FROM q;
