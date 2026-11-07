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
