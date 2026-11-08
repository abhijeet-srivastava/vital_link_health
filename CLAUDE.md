# vital_link_health

Take-home: Readings & Alerting service carved out of the VitalLink monolith.
Design: docs/design.md. Rules and references: the vitallink-service-implementation skill.

- Always load the vitallink-service-implementation skill before touching code, SQL or tests.
- Work one stage at a time from PLAN.md. Stop at each gate and report; do not start the next stage.
- Tests first for anything in references/testing.md. Integration tests use Testcontainers, never mocks of Postgres or Kafka.
- Build: ./mvnw verify. Local stack: docker compose up -d. Tests: ./mvnw test.
- Never edit an applied Flyway migration. Never put PHI (names, contacts, free text) in logs or events
- Precedence: the vitallink-service-implementation skill (references/schema.sql first) > docs/design.md. design.md is background reasoning; where it disagrees with the skill, follow the skill and note the conflict in the stage report. Stop only if the skill contradicts itself.
