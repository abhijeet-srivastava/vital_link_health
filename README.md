# vital_link_health

Readings & Alerting service carved out of the VitalLink monolith (take-home assignment).
Design: [docs/design.md](docs/design.md). Build plan: [PLAN.md](PLAN.md).

## Assumptions

- **Implausible values are rejected.** Each reading is checked against the plausibility bounds in the `measure`
  table (device-error guards, not clinical thresholds), e.g. SpO2 below 50 %, pulse below 20 or above 250 bpm.
  The item comes back `REJECTED` with `IMPLAUSIBLE_VALUE`; the rest of the batch is still stored.
- **Effectively exactly-once.** Delivery is at-least-once end to end; idempotency makes it exactly-once in effect
  for Readings, Alerts, Notifications and Escalations. Deduplication is enforced by the database (unique keys,
  `ON CONFLICT`, conditional `UPDATE`s) rather than by application checks, for robustness and defence in depth:
  - Readings: `(device_id, reading_uuid, measured_at)`; same key with a different `payload_hash` is a `CONFLICT`.
  - Alerts: `alert_occurrence (device_id, reading_uuid, measure_code, measured_at)` counts each reading once;
    `ux_alert_active_episode (patient_id, measure_code) WHERE status <> 'RESOLVED'` keeps one open episode.
  - Notifications: `(trigger_event_id, coach_id, channel)`; `notification_id` is the provider idempotency key.
  - Escalations: `SKIP LOCKED` claim guarded by `status = 'OPEN'`, clearing `escalate_at` in the same `UPDATE`.
- **Devices buffer and resend.** A device may retry the same batch any number of times (after a timeout, a `429`
  or a `503`); resends are reported as `DUPLICATE` and change nothing.
- **Outbox in the same database.** Each entity's outbox table lives in the same database as its data, so an event
  is committed in the same local transaction - no transactions across database servers. Locally a `SKIP LOCKED`
  poller relays the outbox to Kafka; in production this is replaced by CDC (Debezium outbox router).
- **Three independently scalable deployments:** `ingest-api` (write path), `alert-worker` (Kafka consumers:
  evaluator, notifier, escalation, relay) and `query-api` (read path).
- **Authentication:** devices use a per-device API key (`X-Device-Key`); coaches and clinicians use JWT bearer
  tokens (OIDC in production, an HS256 dev issuer locally); service-to-service calls use OAuth2 client credentials.

## Setup, build and run

### 0. Prerequisites

- JDK 21. If your default `java` is newer, point `JAVA_HOME` at 21
  (macOS: `export JAVA_HOME=$(/usr/libexec/java_home -v 21)`).
- Docker with Compose v2, `curl`; Postman is optional.
- k6 is not needed locally: `load/run.sh` runs it in the `grafana/k6:1.3.0` image.

### 1. Build and test

```sh
./mvnw verify      # compile + tests (Testcontainers: Postgres+pg_partman, Kafka, Redis)
./mvnw test        # tests only
```

### 2. Configure the environment (once)

```sh
cp .env.example .env
```

Change the secrets in `.env`. Compose refuses to start (`set in .env`) while any of these is missing:

| Variable | Used for |
| --- | --- |
| `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | Database and owner login (Flyway runs as the owner) |
| `VITALLINK_APP_DB_PASSWORD` | Apps' login `vitallink_app`, created by Flyway's `beforeMigrate` callback |
| `VITALLINK_QUERY_DB_PASSWORD` | query-api coach pool `vitallink_query_app` (row-level security applies) |
| `KAFKA_CLUSTER_ID` | Single-broker KRaft cluster id |
| `VITALLINK_DEVICE_KEY_PEPPER` | Pepper for the HMAC of device API keys |
| `VITALLINK_DEV_JWT_SECRET` | HS256 secret for dev bearer tokens |
| `VITALLINK_LLM_STUB_MODE` | Optional: `ok` (default), `timeout` or `error` |

### 3. Start the stack (includes seed data)

```sh
docker compose up -d --build       # postgres, kafka (KRaft), redis, flyway + seed (one-shot), three services
docker compose ps                  # everything healthy; flyway and seed exited 0
```

Start-up order:

1. `postgres` (16 + pg_partman), `kafka`, `redis`.
2. `flyway` (one-shot) applies the migrations in `readings-alerting/db/src/main/resources/db/migration` and the dev
   seed `db/seed/R__dev_seed.sql`, then exits.
3. `seed` (one-shot, ingest-api image with the `seed` profile) mints API keys for the seeded devices and logs five
   demo keys (devices 1, 2, 601, 602, 851), then exits. Keys are never stored in the repo.
4. `ingest-api`, `alert-worker`, `query-api`. alert-worker creates every topic and its `.dlq` on startup.

Seed data (synthetic IDs only, every insert `ON CONFLICT DO NOTHING`, so reruns change nothing):

- Devices 1-600 BP cuffs, 601-850 glucometers, 851-1000 pulse oximeters; device *i* is bound to patient *i*.
- Patients 1-700 in program 1 (Hypertension), 701-1000 in program 2 (Diabetes), patient 1 in both.
- Coaches 1-20: PRIMARY `((p-1) % 20) + 1`, BACKUP `(p % 20) + 1`. Coach 2 is PRIMARY of patient 2; patient 3 is
  outside coach 2's panel.
- Program threshold bands marked `SEED-NOT-CLINICAL`.

| Service | Port | Health |
| --- | --- | --- |
| ingest-api | 8081 | `curl localhost:8081/actuator/health` |
| alert-worker | 8082 | `curl localhost:8082/actuator/health` |
| query-api | 8083 | `curl localhost:8083/actuator/health` |

Kafka is reachable from the host on `localhost:9094`, Postgres on `5432` and Redis on `6379`, all bound to 127.0.0.1.

Demo device key: `docker compose logs seed | grep 'device_id=2 key='`

### 4. Run the APIs

**Option A - `scripts/demo.sh`** (finds the device key and mints tokens itself; waits for readiness):

| Command | Shows |
| --- | --- |
| `scripts/demo.sh ingest` | Same BP batch posted twice: `ACCEPTED`, then `DUPLICATE`; one reading row |
| `scripts/demo.sh relay` | Outbox row published to `readings.ingested` (key `patient_id`, `event_id` header), then deleted |
| `scripts/demo.sh critical` | Systolic 186 opens one CRITICAL alert with `escalate_at`; a Kafka replay creates nothing new; QUEUE + PUSH notifications |
| `scripts/demo.sh queue` | Coach 2 reads the queue, acknowledges (`200`), stale version (`409`), patient 3 (`403`) |
| `scripts/demo.sh summary` | AI summary from the stub LLM, second call from cache. With `VITALLINK_LLM_STUB_MODE=timeout docker compose up -d`: rules fallback |

**Option B - Postman** (`postman/`):

1. Import `postman/vitallink.postman_collection.json` and `postman/vitallink-local.postman_environment.json`.
2. Select the *VitalLink local* environment; set `jwt_secret` to `VITALLINK_DEV_JWT_SECRET` from `.env` and
   `device_key` to the device 2 key from `docker compose logs seed`.
3. Run the collection top to bottom (Runner): later requests reuse `alert_id`, `alert_version` and `next_cursor`
   captured by earlier ones. Bearer tokens are minted by the collection's pre-request script.

Folders: Health, Ingest (device), Readings (coach), Alerts (coach), AI summary, Access control, Thresholds
(clinician), Internal (service), each with its error cases (400, 401, 403, 409, 422, 428).

**Option C - curl with dev bearer tokens.** query-api validates HS256 JWTs signed with `VITALLINK_DEV_JWT_SECRET`
(the same value compose injects from `.env`). `scripts/mint-dev-token.sh` mints one locally, no running app needed:

```sh
scripts/mint-dev-token.sh <sub> [COACH|CLINICIAN|SERVICE] [scopes] [ttl-seconds]
```

- `sub` - numeric coach id for `COACH`, free-form id otherwise
- actor - defaults to `COACH`
- scopes - space-delimited, defaults to `"readings:read alerts:read alerts:write"`
- ttl-seconds - defaults to `3600`

```sh
# Device ingest (ingest-api)
curl -X POST localhost:8081/v1/devices/2/readings -H "X-Device-Key: <key from seed logs>" \
     -H 'Content-Type: application/json' --data '<batch, see the Postman collection>'

# Coach viewing/acking alerts (query-api)
TOKEN=$(scripts/mint-dev-token.sh 2 COACH "readings:read alerts:read alerts:write")
curl -H "Authorization: Bearer $TOKEN" localhost:8083/v1/coaches/2/alerts
curl -H "Authorization: Bearer $TOKEN" "localhost:8083/v1/patients/2/readings/latest?type=BLOOD_PRESSURE"

# Clinician setting alert thresholds
scripts/mint-dev-token.sh dr-7 CLINICIAN thresholds:write

# Service-to-service call, e.g. internal care-assignment sync
curl --location 'http://localhost:8083/internal/v1/care-assignments' \
  --header "Authorization: Bearer $(scripts/mint-dev-token.sh care-team-sync SERVICE assignments:write)" \
  --header 'Content-Type: application/json' \
  --data '{"patient_id": 2, "coach_id": 2}'
```

Dev tokens are never used in production - real tokens come from the OIDC issuer there.

### 5. Load test

With the stack up (`docker compose up -d`):

```sh
load/run.sh burst                         # 200 devices x 22 requests x 50 readings, ~10% deliberate resends
load/run.sh morning                       # 6-9 AM curve compressed to 9 min, peak 2,800 readings/s
RUN_LABEL=burst-warm load/run.sh burst    # names the result files
RESET_ALERTS=0 load/run.sh burst          # keep open alerts from earlier runs
```

`run.sh` mints keys for devices 1-1000 (minus the demo devices) into the gitignored `load/.keys/keys.csv` on its
first run, resolves open alerts on load patients, samples evaluator lag / outbox backlog / `docker stats`, runs k6 on
the compose network, waits for the pipeline to drain, then runs `load/report.sql` and `load/check-duplicates.sql`
(must be 0). Output: `load/results/<label>-{summary.json,samples.csv,docker-stats.txt,report.txt}`; k6 thresholds
set the exit code. Results and analysis: [LOADTEST.md](LOADTEST.md).

### 6. Day-to-day

```sh
docker compose logs -f alert-worker       # follow one service
docker compose up -d --build ingest-api   # rebuild one service after a code change
docker compose down                       # stop; data kept in the pgdata volume
docker compose down -v                    # wipe the DB; next up re-runs migrations and seed
```

## AI integration: AI reading summary (summary of [AI_INTEGRATION.md](AI_INTEGRATION.md))

`GET /v1/patients/{id}/summary` (query-api) returns a short plain-language summary of a patient's last 24 hours,
always `200`, marked `advisory: true` with a label.

- **Flow:** panel check → Redis `summary:{patientId}` → on a miss, hourly rows from `reading_rollup` (replica,
  row-level security) → de-identified prompt → LLM behind guards → cache 15 min → `ALLOWED` audit. Any LLM problem
  falls back to a rule-based summary (`source: rules`).
- **Data:** `reading_rollup` (count/min/max/mean per patient, measure and UTC hour; daily partitions, 90 days) is
  recomputed by alert-worker's `RollupJob` every 5 min with an idempotent upsert. New alerts evict the cached summary.
- **LLM:** a port (`LlmClient`); only a deterministic stub is wired (`VITALLINK_LLM_STUB_MODE=ok|timeout|error`).
- **Guardrails:**
  - Never on the ingest or evaluation path. In query-api: 2 s timeout, circuit breaker, then rules.
  - Cost: per-pod rate limit (20/s) and daily token budget (5M); over either, rules.
  - PHI: the prompt carries no patient, device or reading ids, no dates (only "h-N") and no free text. Logs never
    hold the prompt or the answer.
  - Access: the same panel check, audit and row-level security as other PHI reads.
  - Advisory only: severity, escalation and notification never read the summary; no diagnosis or treatment advice;
    capped at 1,200 characters.
- **Cost at 1M patients** (placeholder price $1 / $5 per million input / output tokens): ≈ $200/day realistic
  (200k summaries), ≈ $1,000/day if every patient is summarised once a day, ≈ $3,050/day with worst-case prompts.
  Spend is capped by `daily-token-budget × pods`.

## Load test results (summary of [LOADTEST.md](LOADTEST.md))

Runs on 2026-11-09 against `docker compose` on one laptop (Apple M4, 10 cores; one replica per service; k6 on the
compose network). Scenarios: `burst` (200 devices × 22 requests × 50 readings, ~10% deliberate resends) and
`morning` (6-9 AM curve compressed 20× into 9 min, peak 2,800 readings/s).

| | Burst, warm | Morning curve |
| --- | --- | --- |
| Readings stored / sent | 198,600 / 198,600 | 910,505 / 910,505 |
| Throughput | avg 6,297, peak 47,900 rows/s | plateau 2,823 rows/s (target 2,800) |
| Ingest p50 / p95 / p99 | 8.2 / 91 / 162 ms | 1.5 / 9.9 / 106 ms |
| Failed requests | 0 | 0 |
| Shed (429/503), then retried | 141 of 4,400 | 2,415 of ~184,500 (1.3%) |
| Duplicate rows from retries | 0 | 0 (`check-duplicates.sql` = 0) |
| CRITICAL reading → coach queue, p95 | 250 ms | 271 ms (SLO 5 s) |
| Pipeline drain after k6 ends | 1.4 s | 1.2 s |

- **Deadlock found and fixed:** the first runs exposed a virtual-thread pinning deadlock in ingest-api (credential
  cache loaded via JDBC inside Caffeine's `ConcurrentHashMap.compute`). It is now an `AsyncCache` with a dedicated
  loader and jittered expiry; regression test `CachingDeviceDirectoryTest`. All scenarios were re-run after the fix.
- **Targets missed:** cold burst p99 was 540-630 ms and warm burst p99 162 ms, against a 150 ms target stated for
  100-reading batches. The morning run dropped 179 k6 iterations (0.1%).
- **Current bottleneck:** ingest-api per-pod admission (10 DB connections, 64 in flight). Postgres, Kafka and the
  evaluator had headroom.
- **Next steps:** more ingest pods or PgBouncer, a load-aware `Retry-After`, and pre-warming the credential cache.

## Layout

```
pom.xml, mvnw                 Maven parent (Spring Boot 3.5, Java 21)
readings-alerting/
  common/                     domain/, ports/, persistence/, messaging/, observability/
  db/                         Flyway migrations; test-jar with the Testcontainers base classes
  ingest-api/  alert-worker/  query-api/
docker/                       postgres (16 + pg_partman) and app images
docker-compose.yml
```

## Design Decision & Conventions
Correctness and idempotency
- At-least-once delivery, effectively-once effects. Idempotency is guaranteed for Readings, Alerts, Notifications and Escalations. Deduplication happens in the database through natural unique keys with ON CONFLICT or conditional UPDATE, not in the application. Redis and in-memory checks are only fast pre-checks. There are no distributed locks (D3).
- The device owns identity. The device generates reading_uuid, so a buffered or resent reading is recognised as the same measurement. When it's missing, the server derives a UUIDv5.
- Alerts are episodes, not one alert per reading. Each patient and measure has at most one unresolved alert. Repeat breaches are folded in, and severity only ever goes up. This prevents alert floods.

Events and messaging
- Transactional outbox. Each service's outbox lives in the same database as its data, so there are no distributed or two-phase transactions, no lost events and no events without data.
- Only new rows are announced. INSERT … RETURNING writes the outbox row only for readings that were actually inserted, so a resent batch emits nothing.
- Offsets are committed after the database commit. Retries are non-blocking: a failing event goes to a retry topic and then a dead-letter queue, so one bad event never holds up a partition.
- Event ids are UUIDv7, generated by the application. They are unique across shards, time-ordered, and need no database round trip.

Performance and availability
- Ingest never waits on downstream work. Ingest commits and returns 202, and alerting, LLM calls and notifications all run behind Kafka. Under load the service sheds with 503/429 and Retry-After, which is safe because devices buffer.
- CRITICAL alerts are never delayed. They have their own topic and notifier group, reserved rate-limit capacity, and an SMS fallback when the push circuit is open.
- Durability isn't traded for speed. synchronous_commit stays on. Throughput comes from batching.
- Time-series tables are partitioned daily on measured_at. The partition key is in every unique key, and pagination is keyset-based, never OFFSET.

Concurrency
- State changes are conditional updates. They use optimistic version checks plus legal source states. A stale action gets 409 with the current state.
- Schedulers are safe to run on many pods. The escalation scheduler and notifier senders claim work with FOR UPDATE SKIP LOCKED and a status guard.

Conventions used in Coding
- Packages named by architecture layer, Ports(interfaces) named after business rule (i.e. ReadingRepository, AlertService, LlmClient, DeliveryProvider)
- DB objects named as ix_ for an index, ux_ for a unique index, <table>_<cols> style, Kafka topics named as <entity>.<past-tense verb>, with .retry, .dlq and .critical suffixes(i.e. readings.ingested, alerts.raised.critical, readings.ingested.dlq)
- Error Handling: Domain code throws domain exceptions and only the API layer maps them to HTTP. For example, DeviceMismatchException becomes 403 and MalformedBatchException becomes 422.
- A batch can partly fail. Each item gets its own status: ACCEPTED, DUPLICATE, CONFLICT or REJECTED + code. The request is 202 if at least one item was accepted.

API and contract versioning

- REST:
  - Every path is under /v1, and internal endpoints under /internal/v1.
  - A breaking change means adding /v2 next to /v1. The meaning of an existing field never changes.
- Events:
  - Every payload has a schema_version.
  - Payloads carry ids and values only.

## Tradeoff during Design
 - Ingest API shed load with 503 error: Device can buffer and resend events, so if IN_FLIGHT request breach threshold, device are advice to emit again with a retry with jitter
 - Reading older than 7 days are recorded as LOW priority and later than 90 days are rejected.
 - Device generated reading_id, plus measured_at to ensure idempotency of readings.
 - Batching evaluation of alerts: Each consumers takes upto 500 events and write 1 transaction per event, At peak of 3000 events per/sec, with 64 consumers and 5-10 batch, translates to 300-600 commits/s
 - Separate handling of critical alerts: Separate topic for critical alerts


## Backpressure

ingest-api sheds load before reading the body or touching the database (`vitallink.ingest.backpressure.*`):

- `429 RATE_LIMITED` + `Retry-After`: per-device token bucket in Redis (`device-burst`, `device-requests-per-minute`).
  If Redis is down the limiter lets requests through; Redis is never required for correctness.
- `503 OVERLOADED` + jittered `Retry-After` (`retry-after-min`..`max`), `reason`:
  - `IN_FLIGHT`: `max-in-flight` requests already running on this pod;
  - `OUTBOX_BACKLOG`: the outbox has more than `max-outbox-rows` rows or its oldest row is older than
    `max-outbox-age` (relay or Kafka down), sampled every `outbox-sample-interval`.

Devices buffer and retry, so shedding loses nothing. With the relay stopped, ingest starts returning `503` about
`max-outbox-age` (60 s) later.

## Outbox relay

Ingest writes a `readings.ingested` row to `outbox_event` in the same transaction as the readings; nothing on the
request path talks to Kafka. Locally, the `relay` profile of alert-worker (one replica in compose) publishes those rows:

- one transaction per batch: `SELECT … ORDER BY created_at LIMIT 500 FOR UPDATE SKIP LOCKED`, publish each row to the
  topic stored in it with key = `aggregate_key` (patient_id; device_id for binding events) and the `event_id` header,
  wait for every broker ack, then delete the rows and commit;
- idempotent producer with `acks=all`, forced in code so configuration cannot weaken it;
- at-least-once: a crash after publish and before the delete re-publishes the same `event_id`; consumers dedupe on
  natural keys or `event_id`. Several relays can run at once (`SKIP LOCKED` keeps their batches disjoint).

alert-worker creates every topic and its `.dlq` on startup (`KafkaTopics`; partition counts from the architecture
reference). `scripts/demo.sh relay` shows a published record with its key and headers.

**Production uses Debezium with the outbox event router instead** (CDC from the WAL, routed by the `topic` column,
keyed by `aggregate_key`, `event_id` as a header). The table and the consumers stay the same; Debezium is not built
here.

## How I approached to Problem:
I used Claude Code to generate the design scaffolding and code, but I kept the core logic in my own hands and put a strong harness around the generated code: every stage had to pass a gate check before I moved on.

  - I started with a short prompt explaining the problem statement, the key flows and the constraints from Part A of the assignment.
  - I explained my approach: an asynchronous, event-driven pipeline for alert evaluation and notification, so that ingestion is never blocked.
  - I shared the key APIs and entities I wanted in the design, and asked it to generate a low-level design using Java 21, Spring Boot and Docker Compose.
  - I validated the generated design and made the changes it needed, such as adding unique keys in the database.
  - I asked it to plan the implementation in stages:
    - Scaffold and local stack
    - Schema, partitions and seed data
    - Ingest API
    - Outbox relay and topics
    - Threshold evaluator
    - Alert state changes and escalation
    - Notifier
    - Query API
  - Each stage had a defined scope and its own validation gate checks.
  - Each stage start with writing test cases around expected output of that stage, these test cases are functional, and concurrency, failure edge case as well.
  - I manually reviewed test cases, added missing behavior wherever required.
  - Then prompted agent to generate implementation to satisfy test cases.
  - Each stage has it own harness abd evolution, making its implementation most rigorous and error proof.
  - After each stage, I ran a gate check to confirm the stage’s output matched the plan.
  - Finally, I ran an independent review through a Claude subagent, validating the work across all stages