#!/usr/bin/env bash
# Demo against the compose stack (docker compose up -d). Subcommands are added stage by stage.
#   scripts/demo.sh ingest   same BP batch posted twice -> ACCEPTED, then DUPLICATE; one reading row
#   scripts/demo.sh relay    ingest, then the relay publishes one readings.ingested record (key = patient_id,
#                            event_id header) and deletes its outbox row
set -euo pipefail
cd "$(dirname "$0")/.."

INGEST_URL=${INGEST_URL:-http://localhost:8081}
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

case "${1:-}" in
  ingest) ingest ;;
  relay) relay ;;
  *) echo "usage: $0 ingest|relay" >&2; exit 2 ;;
esac
