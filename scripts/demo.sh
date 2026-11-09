#!/usr/bin/env bash
# Demo against the compose stack (docker compose up -d). Subcommands are added stage by stage.
#   scripts/demo.sh ingest   same BP batch posted twice -> ACCEPTED, then DUPLICATE; one reading row
#   scripts/demo.sh relay    ingest, then the relay publishes one readings.ingested record (key = patient_id,
#                            event_id header) and deletes its outbox row
#   scripts/demo.sh critical systolic 186 -> one CRITICAL alert with escalate_at; the same readings.ingested record
#                            replayed to Kafka -> still one alert, occurrence_cnt = 1; the notifier writes QUEUE +
#                            PUSH (both SENT) for the primary coach; the alerts.raised.critical record replayed -> no
#                            new notification
#   scripts/demo.sh queue    coach 2 (PRIMARY of patient 2) reads their queue with a dev token and sees the patient's
#                            unresolved SYSTOLIC alert (running 'critical' first when there is none); acknowledges it
#                            with the current version (200), then again with the now stale one (409 + current alert);
#                            and the same coach reading patient 3 (not in the panel) gets 403
#   scripts/demo.sh summary  ingest a reading, wait for alert-worker's RollupJob to roll it up, then coach 2 asks for
#                            patient 2's AI summary twice (stub LLM: source llm, the second from the 15 min cache).
#                            With VITALLINK_LLM_STUB_MODE=timeout docker compose up -d: source rules after ~2 s
set -euo pipefail
cd "$(dirname "$0")/.."

INGEST_URL=${INGEST_URL:-http://localhost:8081}
QUERY_URL=${QUERY_URL:-http://localhost:8083}
COACH_ID=2            # dev seed: PRIMARY of patient 2 ((p - 1) % 20 + 1), BACKUP of patient 1 (p % 20 + 1)
OUTSIDE_PATIENT_ID=3  # PRIMARY coach 3, BACKUP coach 4: not in coach 2's panel
DEVICE_ID=2      # dev seed: device 2 is bound to patient 2 and is a demo device (key logged by the seed service)
PATIENT_ID=2

# Demo keys are minted and logged by the one-shot seed service; the newest run wins.
device_key() {
  docker compose logs --no-log-prefix seed \
    | grep -o "device_id=$1 key=vl_[0-9a-f]\{16\}_[A-Za-z0-9_-]*" | tail -1 | sed 's/.*key=//'
}

psql_value() {
  docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "$0"' "$1"
}

# `docker compose up -d` returns once containers start, before the apps listen; the port proxy then answers with an
# empty reply. Wait for readiness so `docker compose up -d && scripts/demo.sh ...` works as one command.
wait_ready() {
  local url=$1 deadline=$((SECONDS + ${READY_TIMEOUT:-90}))
  until curl -sf "$url/actuator/health/readiness" > /dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "$url not ready after ${READY_TIMEOUT:-90}s; see: docker compose logs ingest-api" >&2
      exit 1
    fi
    sleep 1
  done
}

post_batch() {
  curl -sS -X POST "$INGEST_URL/v1/devices/$DEVICE_ID/readings" \
    -H "Content-Type: application/json" -H "X-Device-Key: $1" --data "$2"
  echo
}

ingest() {
  local key reading_id measured_at body
  wait_ready "$INGEST_URL"
  key=$(device_key "$DEVICE_ID")
  [ -n "$key" ] || { echo "No demo key for device $DEVICE_ID in 'docker compose logs seed'" >&2; exit 1; }
  reading_id=$(uuidgen | tr '[:upper:]' '[:lower:]')
  measured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  body=$(cat <<JSON
{"patient_id": $PATIENT_ID, "readings": [
  {"reading_id": "$reading_id", "device_seq": 1, "type": "BLOOD_PRESSURE", "measured_at": "$measured_at",
   "values": {"systolic": 186, "diastolic": 94, "pulse": 78}, "unit": "mmHg"}]}
JSON
)
  echo "== first post (expect ACCEPTED)"
  post_batch "$key" "$body"
  echo "== retry of the same batch (expect DUPLICATE)"
  post_batch "$key" "$body"
  echo "== reading rows for $reading_id (expect 1)"
  psql_value "SELECT count(*) FROM reading WHERE reading_uuid = '$reading_id'"
  READING_ID=$reading_id
}

relay() {
  local deadline records
  ingest
  echo "== waiting for the relay to publish and delete the outbox row"
  deadline=$((SECONDS + 30))
  until [ "$(psql_value "SELECT count(*) FROM outbox_event WHERE payload->'readings' @> '[{\"reading_uuid\": \"$READING_ID\"}]'")" = 0 ]; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "outbox row still present after 30s; see: docker compose logs alert-worker" >&2
      exit 1
    fi
    sleep 1
  done
  echo "== readings.ingested records carrying reading $READING_ID (expect 1; key = $PATIENT_ID, event_id header)"
  records=$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic readings.ingested --from-beginning --timeout-ms 10000 \
    --property print.key=true --property print.headers=true 2>/dev/null | grep "$READING_ID" || true)
  echo "$records"
  echo "count: $(printf '%s' "$records" | grep -c "$READING_ID" || true)"
}

alert_for_reading() {
  psql_value "SELECT a.alert_id, a.severity, a.status, a.escalate_at IS NOT NULL AS escalate_at_set, a.occurrence_cnt
              FROM alert a JOIN alert_occurrence o USING (alert_id)
              WHERE o.reading_uuid = '$READING_ID' AND o.measure_code = 1"
}

notifications_for_alert() {
  psql_value "SELECT coach_id, channel, reason, severity, status, attempts FROM notification
              WHERE alert_id = $1 ORDER BY channel, coach_id"
}

wait_group_caught_up() {
  local group=$1 topic=$2 deadline=$((SECONDS + 30))
  until docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
      --describe --group "$group" 2>/dev/null \
      | awk -v t="$topic" '$2 == t && $6 != "0" && $6 != "-" { lag = 1 } END { exit lag }'; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "$group still lagging on $topic after 30s; see: docker compose logs alert-worker" >&2
      exit 1
    fi
    sleep 1
  done
}

wait_evaluator_caught_up() {
  local deadline=$((SECONDS + 30))
  until docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
      --describe --group alert-evaluator 2>/dev/null \
      | awk '$2 == "readings.ingested" && $6 != "0" && $6 != "-" { lag = 1 } END { exit lag }'; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "alert-evaluator still lagging after 30s; see: docker compose logs alert-worker" >&2
      exit 1
    fi
    sleep 1
  done
}

critical() {
  local deadline record
  # Demo reset: resolve the patient's open SYSTOLIC episode (e.g. from 'ingest'), so this run opens a fresh one.
  psql_value "UPDATE alert SET status = 'RESOLVED', resolved_at = now(), escalate_at = NULL, version = version + 1,
              resolution_note = 'demo reset'
              WHERE patient_id = $PATIENT_ID AND measure_code = 1 AND status <> 'RESOLVED'" > /dev/null
  relay
  echo "== waiting for the evaluator to open the alert"
  deadline=$((SECONDS + 30))
  until [ -n "$(alert_for_reading)" ]; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "no alert after 30s; see: docker compose logs alert-worker" >&2
      exit 1
    fi
    sleep 1
  done
  echo "== alert_id|severity|status|escalate_at_set|occurrence_cnt (expect CRITICAL|OPEN|t|1)"
  alert_for_reading
  echo "== replaying the same readings.ingested record (same event_id, same readings)"
  record=$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic readings.ingested --from-beginning --timeout-ms 10000 \
    --property print.key=true --property print.headers=true 2>/dev/null | grep "$READING_ID" | head -1)
  printf '%s\n' "$record" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic readings.ingested \
    --property parse.key=true --property parse.headers=true
  wait_evaluator_caught_up
  echo "== after replay (expect the same row, occurrence_cnt still 1)"
  alert_for_reading
  echo "== alerts for patient $PATIENT_ID SYSTOLIC not resolved (expect 1)"
  psql_value "SELECT count(*) FROM alert WHERE patient_id = $PATIENT_ID AND measure_code = 1 AND status <> 'RESOLVED'"
  notify "$(alert_for_reading | cut -d'|' -f1)"
}

notify() {
  local alert_id=$1 deadline before after record
  echo "== waiting for the notifier (QUEUE + PUSH SENT for the primary coach)"
  deadline=$((SECONDS + 30))
  until [ "$(psql_value "SELECT count(*) FROM notification WHERE alert_id = $alert_id AND status = 'SENT'")" -ge 2 ]; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "no notifications after 30s; see: docker compose logs alert-worker" >&2
      exit 1
    fi
    sleep 1
  done
  echo "== coach_id|channel|reason|severity|status|attempts (expect QUEUE and PUSH, CRITICAL, SENT, primary coach)"
  notifications_for_alert "$alert_id"
  before=$(psql_value "SELECT count(*) FROM notification WHERE alert_id = $alert_id")
  echo "== replaying the alerts.raised.critical record (same event_id)"
  record=$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic alerts.raised.critical --from-beginning --timeout-ms 10000 \
    --property print.key=true --property print.headers=true 2>/dev/null | grep -E "\"alert_id\": ?$alert_id[,}]" | tail -1)
  printf '%s\n' "$record" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic alerts.raised.critical \
    --property parse.key=true --property parse.headers=true
  wait_group_caught_up notifier-critical alerts.raised.critical
  after=$(psql_value "SELECT count(*) FROM notification WHERE alert_id = $alert_id")
  echo "== notifications for the alert before|after replay (expect equal)"
  echo "$before|$after"
}

ackable_systolic_alert() {
  psql_value "SELECT alert_id || '|' || version FROM alert
              WHERE patient_id = $PATIENT_ID AND measure_code = 1 AND status IN ('OPEN', 'ESCALATED')"
}

queue() {
  local row alert_id version token
  wait_ready "$QUERY_URL"
  row=$(ackable_systolic_alert)
  # An episode acknowledged by an earlier run cannot be acknowledged again; critical() resets it and opens a new one.
  if [ -z "$row" ]; then
    critical
    row=$(ackable_systolic_alert)
  fi
  alert_id=${row%%|*}
  version=${row##*|}
  token=$(scripts/mint-dev-token.sh "$COACH_ID")
  echo "== coach $COACH_ID queue (expect alert $alert_id for patient $PATIENT_ID; CRITICAL first, then oldest)"
  curl -sS "$QUERY_URL/v1/coaches/$COACH_ID/alerts" -H "Authorization: Bearer $token"
  echo
  echo "== acknowledge alert $alert_id with If-Match: \"$version\" (expect 200, ETag = version + 1)"
  curl -sS -i -X POST "$QUERY_URL/v1/alerts/$alert_id/acknowledge" -H "Authorization: Bearer $token" \
    -H "If-Match: \"$version\"" | grep -iE '^HTTP|^ETag|"status"'
  echo
  echo "== acknowledge again with the stale If-Match: \"$version\" (expect 409 with the current alert)"
  curl -sS -i -X POST "$QUERY_URL/v1/alerts/$alert_id/acknowledge" -H "Authorization: Bearer $token" \
    -H "If-Match: \"$version\"" | grep -iE '^HTTP|^ETag|"code"'
  echo
  echo "== coach $COACH_ID reading patient $OUTSIDE_PATIENT_ID, who is not in the panel (expect 403, audited DENIED)"
  curl -sS -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $token" \
    "$QUERY_URL/v1/patients/$OUTSIDE_PATIENT_ID/readings/latest?type=BLOOD_PRESSURE"
  psql_value "SELECT action, outcome, patient_id FROM audit_log
              WHERE actor_type = 'COACH' AND actor_id = '$COACH_ID' ORDER BY at DESC LIMIT 4"
}

summary() {
  local token deadline
  wait_ready "$QUERY_URL"
  ingest
  echo "== waiting for the rollup job to cover the reading (reading_rollup, every 15 s in compose)"
  deadline=$((SECONDS + 60))
  until [ "$(psql_value "SELECT count(*) FROM reading_rollup WHERE patient_id = $PATIENT_ID
                         AND updated_at >= (SELECT received_at FROM reading WHERE reading_uuid = '$READING_ID')")" -gt 0 ]; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "no rollup after 60s; see: docker compose logs alert-worker" >&2
      exit 1
    fi
    sleep 1
  done
  # Demo reset: drop a summary cached by an earlier run, so this run shows the current stub mode.
  docker compose exec -T redis redis-cli DEL "summary:$PATIENT_ID" > /dev/null
  token=$(scripts/mint-dev-token.sh "$COACH_ID")
  echo "== coach $COACH_ID asks for patient $PATIENT_ID's summary (stub ok: source llm; stub timeout: source rules after ~2 s)"
  curl -sS -w '\nhttp %{http_code} in %{time_total}s\n' -H "Authorization: Bearer $token" \
    "$QUERY_URL/v1/patients/$PATIENT_ID/summary"
  echo "== again (an llm summary now comes from the cache, summary:$PATIENT_ID; a rules one is never cached)"
  curl -sS -w '\nhttp %{http_code} in %{time_total}s\n' -H "Authorization: Bearer $token" \
    "$QUERY_URL/v1/patients/$PATIENT_ID/summary"
  echo "== coach $COACH_ID asks for patient $OUTSIDE_PATIENT_ID's summary, not in the panel (expect 403, audited DENIED)"
  curl -sS -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $token" \
    "$QUERY_URL/v1/patients/$OUTSIDE_PATIENT_ID/summary"
  psql_value "SELECT action, outcome, patient_id FROM audit_log
              WHERE actor_type = 'COACH' AND actor_id = '$COACH_ID' AND action = 'READ_SUMMARY' ORDER BY at DESC LIMIT 3"
}

case "${1:-}" in
  ingest) ingest ;;
  relay) relay ;;
  critical) critical ;;
  queue) queue ;;
  summary) summary ;;
  *) echo "usage: $0 ingest|relay|critical|queue|summary" >&2; exit 2 ;;
esac
