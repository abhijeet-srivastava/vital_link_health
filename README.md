# vital_link_health

Readings & Alerting service carved out of the VitalLink monolith (take-home assignment).
Design: [docs/design.md](docs/design.md). Build plan: [PLAN.md](PLAN.md).

## Build and run

Requires JDK 21 and Docker. If your default `java` is newer, point `JAVA_HOME` at 21
(macOS: `export JAVA_HOME=$(/usr/libexec/java_home -v 21)`).

```sh
./mvnw verify                      # compile + tests (Testcontainers: Postgres+pg_partman, Kafka, Redis)
cp .env.example .env               # once; change POSTGRES_PASSWORD
docker compose up -d --build       # postgres, kafka (KRaft), redis, flyway (one-shot), three services
docker compose ps                  # everything healthy, flyway exited 0
```

| Service | Port | Health |
| --- | --- | --- |
| ingest-api | 8081 | `curl localhost:8081/actuator/health` |
| alert-worker | 8082 | `curl localhost:8082/actuator/health` |
| query-api | 8083 | `curl localhost:8083/actuator/health` |

Kafka is reachable from the host on `localhost:9094`, Postgres on `5432` and Redis on `6379`, all bound to 127.0.0.1.

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

