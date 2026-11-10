#!/usr/bin/env bash
# Run one k6 script against the compose stack and collect the pipeline numbers around it.
#   load/run.sh burst|morning [-e NAME=value ...]     (extra args go to k6 run)
#   RUN_LABEL=burst-cold load/run.sh burst              (names the result files; defaults to the script name)
# Needs: docker compose up -d. First run mints load/.keys/keys.csv (gitignored) through the seed service.
# Writes load/results/<name>-{summary.json,samples.csv,docker-stats.txt,report.txt}.
set -euo pipefail
cd "$(dirname "$0")/.."

name=${1:?usage: load/run.sh burst|morning [k6 args]}
shift
label=${RUN_LABEL:-$name}
K6_IMAGE=grafana/k6:1.3.0
GROUP=alert-evaluator
TOPIC=readings.ingested
out=load/results
mkdir -p "$out" load/.keys

psql_value() {
  docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "$0"' "$1"
}

evaluator_lag() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group "$GROUP" 2>/dev/null \
    | awk -v t="$TOPIC" '$2 == t && $6 ~ /^[0-9]+$/ { lag += $6 } END { print lag + 0 }'
}

if [ ! -s load/.keys/keys.csv ]; then
  echo "== minting load-test device keys (seed service -> load/.keys/keys.csv)"
  docker compose run --rm --no-deps -e VITALLINK_SEED_LOAD_KEYS_FILE=/keys/keys.csv \
    -v "$PWD/load/.keys:/keys" seed > /dev/null
  docker compose restart ingest-api > /dev/null   # drop cached credentials of the rotated keys
  until curl -sf http://localhost:8081/actuator/health/readiness > /dev/null; do sleep 1; done
fi

# Each run starts with no unresolved alerts on the load patients (as demo.sh's reset does), so CRITICAL readings
# open new episodes and the ingest -> queue latency has something to measure. RESET_ALERTS=0 keeps them.
if [ "${RESET_ALERTS:-1}" = 1 ]; then
  psql_value "UPDATE alert SET status = 'RESOLVED', resolved_at = now(), escalate_at = NULL, version = version + 1,
              resolution_note = 'load test reset'
              WHERE status <> 'RESOLVED' AND patient_id NOT IN (1, 2, 601, 602, 851)" > /dev/null
fi

network=$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' \
  "$(docker compose ps -q ingest-api)")

# Samplers: evaluator lag (records) and outbox backlog every ~2 s; container CPU/memory every ~2 s.
samples="$out/$label-samples.csv"
echo "ts,evaluator_lag,outbox_rows,outbox_oldest_s" > "$samples"
( while :; do
    lag=$(evaluator_lag)
    ob=$(psql_value "SELECT count(*) || ',' || coalesce(round(extract(epoch FROM now() - min(created_at)), 1), 0)
                     FROM outbox_event")
    echo "$(date -u +%H:%M:%S),$lag,$ob" >> "$samples"
  done ) &
sampler=$!
: > "$out/$label-docker-stats.txt"
( while :; do
    docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' \
      | sed "s/^/$(date -u +%H:%M:%S) /" >> "$out/$label-docker-stats.txt"
  done ) &
stats=$!
trap 'kill $sampler $stats 2>/dev/null || true' EXIT

commits0=$(psql_value "SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()")
t0=$(psql_value "SELECT now()")
echo "== k6 $name as $label (t0=$t0)"
set +e
docker run --rm --network "$network" -v "$PWD/load:/load" "$K6_IMAGE" run --quiet -e RUN_LABEL="$label" "/load/$name.js" "$@"
k6_status=$?
set -e
t1=$(psql_value "SELECT now()")

echo "== waiting for the pipeline to drain (outbox empty, evaluator lag 0)"
deadline=$((SECONDS + 900))
until [ "$(psql_value "SELECT count(*) FROM outbox_event")" = 0 ] && [ "$(evaluator_lag)" = 0 ]; do
  [ "$SECONDS" -lt "$deadline" ] || { echo "not drained after 15 min" >&2; break; }
  sleep 1
done
t2=$(psql_value "SELECT now()")
commits1=$(psql_value "SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()")
sleep 3   # let the last sample land
{ kill $sampler $stats && wait $sampler $stats; } 2>/dev/null || true

{
  echo "t0 (k6 start)  $t0"
  echo "t1 (k6 end)    $t1"
  echo "t2 (drained)   $t2"
  psql_value "SELECT 'k6 duration s  ' || round(extract(epoch FROM '$t1'::timestamptz - '$t0'::timestamptz), 1)
              || E'\ndrain after k6 s ' || round(extract(epoch FROM '$t2'::timestamptz - '$t1'::timestamptz), 1)
              || E'\nall DB commits/s (t0..t2) ' || round(($commits1 - $commits0)
                 / extract(epoch FROM '$t2'::timestamptz - '$t0'::timestamptz))"
  awk -F, 'NR > 1 { if ($2 > lag) lag = $2; if ($3 > ob) ob = $3; if ($4 > age) age = $4 }
           END { printf "max evaluator lag (records) %d\nmax outbox rows %d\nmax outbox oldest s %s\n", lag, ob, age }' \
    "$samples"
  docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v t0="$0" -v t1="$1" -v t2="$2"' \
    "$t0" "$t1" "$t2" < load/report.sql
  echo "== check-duplicates.sql (whole database, must be 0)"
  docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA' < load/check-duplicates.sql
} | tee "$out/$label-report.txt"
exit $k6_status
