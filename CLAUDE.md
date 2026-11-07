# vital_link_health

Take-home: Readings & Alerting service carved out of the VitalLink monolith.
Design: docs/design.md. Rules and references: the vitallink-service-implementation skill.

- Always load the vitallink-service-implementation skill before touching code, SQL or tests.
- Work one stage at a time from PLAN.md. Stop at each gate and report; do not start the next stage.
- Tests first for anything in references/testing.md. Integration tests use Testcontainers, never mocks of Postgres or Kafka.
- Build: ./gradlew build. Local stack: docker compose up -d. Tests: ./gradlew test.
- Never edit an applied Flyway migration. Never put PHI (names, contacts, free text) in logs or events.
- If a request conflicts with the skill or docs/design.md, stop and say so.
