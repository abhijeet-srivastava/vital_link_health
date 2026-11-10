# PLAN.md — Readings & Alerting service

Approved 2026-11-07. Work one stage at a time; stop at each gate and report; do not start the next stage.

## Context

Take-home (`HBox_Platform_Principal_Engineer_Assignment.pdf`): carve Readings & Alerting out of the VitalLink monolith. Deliverables: Part A service, Part B design doc (`docs/design.md`), Part C AI feature, Part D and E write-ups. The assignment favours "a smaller, well-built, well-explained system" over breadth (3–4 days), so this plan builds a **lean core**: the full end-to-end path (ingest → dedup → persist → async alert → notification → read APIs) done properly. Clinical volume controls and dashboards are flags or documentation, not code.

Precedence (CLAUDE.md): the skill (`references/schema.sql` first) > `docs/design.md`. Where they disagree, follow the skill and note it in the stage report.

## Ground rules for every stage

- Load the `vitallink-service-implementation` skill first; read only the references the stage touches.
- **Tests first** for every `references/testing.md` row the stage covers, then code.
- Integration tests: Testcontainers (Postgres + pg_partman image, Kafka, Redis). No mocks of Postgres or Kafka. One container set per class, truncate between tests, `CountDownLatch` start gates, injected `Clock`, Awaitility, no `Thread.sleep`.
- Write paths: `JdbcClient` with explicit SQL. No JPA on ingest, evaluator or notifier.
- Migrations: `V<yyyymmddhhmm>__<verb>_<object>.sql`, migration checklist applied, converge to `schema.sql`. Never edit an applied migration.
- No PHI in logs, metric labels, exceptions or events.
- Stage report = gate output + 3–5 line design delta + skill review checklist on the stage diff + any skill/design.md conflicts found.
- Git: feature branch `feat/readings-alerting` off `main`; commit(s) per stage after its gate passes.

## Layout (Maven multi-module, per SKILL.md "Code layout")

```
pom.xml (parent), mvnw, .mvn/
readings-alerting/
  common/        domain/, ports/, persistence/, messaging/, observability/ (PHI log scrubber only)   (com.vitallink.readings.*)
  db/            src/main/resources/db/migration (Flyway); test-jar: Testcontainers base classes
  ingest-api/    Spring Boot app
  alert-worker/  Spring Boot app; profiles: evaluator, notifier, notifier-critical, scheduler, relay
  query-api/     Spring Boot app (coach/clinician APIs, summary, internal admin)
  load/          k6 script
docker/postgres/Dockerfile   postgres:16 + postgresql-16-partman
docker-compose.yml           postgres, kafka (KRaft), redis, flyway (one-shot), kafka-init, apps
scripts/                     demo.sh (happy + duplicate path), mint-dev-token.sh
```

- Flyway runs from a one-shot container as the owner role; apps use a least-privilege role (INSERT-only on `audit_log`).
- Local outbox relay = `relay` profile of alert-worker, single replica (Debezium in production, D2).
- `docker compose up -d` brings everything up with seed data (the assignment's "one command").

---

## Stage 1 — Scaffold + docker-compose

**References**: architecture.md, security-phi.md (secrets).

**Scope**: Maven parent + modules, Java 21, Spring Boot 3, dependency management in the parent POM. Three bootable apps with actuator health. Shared Testcontainers base in `db` test-jar. Compose stack with healthchecks. `.env.example` only. JSON logging with a PHI-field scrubber stub. Update `CLAUDE.md` build/test lines to `./mvnw` and add `./mvnw`/`k6` to `.claude/settings.json` (both agreed with you).

**Files**: `pom.xml`, `readings-alerting/*/pom.xml`, `mvnw`, `.mvn/wrapper/*`, `*Application.java` ×3, `application.yml` ×3, `docker-compose.yml`, `docker/postgres/Dockerfile`, `.env.example`, `.gitignore` (adds `.idea/`, `*.iml`), `db/src/test/java/.../IntegrationTestBase.java`, `logback-spring.xml`.

**Tests first**: `containers_start_and_postgres_has_partman_and_btree_gist`; `context_loads` per app.

**Gate**:
```
./mvnw verify
docker compose up -d && docker compose ps     # all healthy
curl -sf localhost:8081/actuator/health && curl -sf localhost:8082/actuator/health && curl -sf localhost:8083/actuator/health
```

## Stage 2 — Schema + seed

**References**: schema.sql, migration_checklist.sql, security-phi.md.

**Scope**: Migrations converging exactly to `schema.sql`. pg_partman daily partitions (7 ahead, 90-day retention) for `reading`, `alert_occurrence`, `audit_log`, plus DEFAULT partitions. Roles and grants. `measure` rows as a real migration. Dev seed (separate Flyway location, compose and tests only): one BP program with default bands (e.g. systolic GT 160 HIGH, GT 180 CRITICAL, LT 90 CRITICAL), a second program for one patient (multi-program case), devices + credentials (dev pepper; keys printed in README), bindings including one rebinding, coaches with PRIMARY and BACKUP assignments. Partition maintenance on the `scheduler` profile.

**Files**: `db/migration/V…__create_extensions.sql`, `V…__create_reference_and_device_tables.sql`, `V…__create_projection_and_threshold_tables.sql`, `V…__create_reading_partitioned.sql`, `V…__create_outbox_and_processed_event.sql`, `V…__create_alert_tables.sql`, `V…__create_notification.sql`, `V…__create_audit_log.sql`, `V…__create_roles_and_grants.sql`, `V…__insert_measures.sql`; `db/seed/R__dev_seed.sql`; `PartitionMaintenanceJob.java`.

**Tests first**:
- `schema_matches_reference_schema`
- `overlapping_device_bindings_are_rejected`
- `second_unresolved_episode_for_same_patient_and_measure_is_rejected`
- `escalate_at_on_non_open_or_non_critical_alert_is_rejected`
- `critical_rule_with_confirm_n_above_one_is_rejected`
- `service_role_cannot_update_or_delete_audit_log`
- `partitions_exist_seven_days_ahead_and_default_is_empty`

**Gate**: `./mvnw -pl readings-alerting/db verify`; `docker compose up -d && docker compose exec postgres psql -U vitallink -c '\d+ reading'` lists daily partitions.

## Stage 3 — ingest-api

**References**: idempotency-and-concurrency.md (pattern 1), api-contracts.md, security-phi.md, Ports.java.

**Scope**: `POST /v1/devices/{deviceId}/readings` (single = batch of one). `X-Device-Key vl_<key_id>_<secret>`, constant-time HMAC compare, Caffeine + Redis `dev:{keyId}` cache, DB fallback behind a circuit breaker. Binding checked at each `measured_at` (`403`). Validation: 1–500 items (`413`), plausibility from `measure`, `FUTURE_TIMESTAMP`, `READING_TOO_OLD` (flag), UUIDv5 when `reading_id` is absent. One transaction: `unnest` insert `ON CONFLICT DO NOTHING RETURNING`; `payload_hash` → `DUPLICATE`/`CONFLICT`; one `readings.ingested` outbox row for new rows only. `202` + per-item status. After commit: best-effort Redis Lua CAS on `latest:{patient}:{type}`. RFC 7807. Backpressure: in-flight semaphore → `503 Retry-After`; per-device token bucket → `429`. Audit row per ingest request.

**Files**: `common/domain/{Reading,ReadingType,Measure,IngestBatch,IngestResult,ItemStatus}.java`, `common/ports/*`, `common/persistence/{JdbcReadingRepository,JdbcOutboxWriter,JdbcDeviceCredentialRepository,JdbcAuditWriter}.java`, `common/messaging/{UuidV7,ReadingsIngestedV1}.java`, `ingest-api/api/{ReadingsController,ProblemDetailsAdvice}.java`, `ingest-api/auth/{HmacDeviceAuthenticator,CachingDeviceAuthenticator}.java`, `ingest-api/service/DefaultReadingIngestService.java`, `ingest-api/cache/RedisLatestReadingCache.java` + `latest_cas.lua`, `ingest-api/backpressure/{InFlightLimiter,DeviceRateLimiter}.java`.

**Tests first** (testing.md: Ingest idempotency, Ingest integrity, Binding, Latest reading):
- `retried_batch_sent_concurrently_from_two_threads_is_stored_once_and_announced_once`
- `same_reading_id_with_different_values_is_conflict_and_original_row_unchanged`
- `device_bound_to_patient_a_posting_for_patient_b_gets_403`
- `reading_measured_before_rebinding_is_stored_for_old_patient`
- `out_of_order_readings_never_overwrite_newer_latest_cache_entry`
- `bad_key_401`, `batch_over_500_413`, `future_timestamp_item_rejected`, `reading_older_than_90_days_rejected_when_flag_on`, `redis_down_ingest_still_returns_202`
- `outbox_payload_and_logs_contain_no_phi_fields`

**Gate**: `./mvnw -pl readings-alerting/ingest-api -am verify`; with compose up, `scripts/demo.sh ingest` posts the same BP batch twice → `ACCEPTED` then `DUPLICATE`, and `select count(*) from outbox_event` = 1.

## Stage 4 — Outbox relay + topics

**References**: architecture.md (topics), idempotency-and-concurrency.md (pattern 7).

**Scope**: Topics with the architecture table's partition counts plus `.dlq` topics (compose `kafka-init` and the test base). Relay: `SELECT … ORDER BY created_at LIMIT 500 FOR UPDATE SKIP LOCKED`, publish with key = `aggregate_key` and `event_id` header, idempotent producer (`acks=all`), delete after ack in the same transaction. Outbox backlog gauge feeds ingest backpressure.

**Files**: `common/messaging/{OutboxRelay,KafkaTopics,EventHeaders}.java`, `alert-worker/config/RelayConfig.java`, `docker/kafka/create-topics.sh`.

**Tests first**:
- `outbox_row_is_published_keyed_by_patient_with_event_id_header_and_deleted`
- `kafka_down_outbox_grows_and_drains_in_order_when_it_returns`
- `crash_after_publish_before_delete_republishes_same_event_id`
- `topics_have_the_agreed_partition_counts`

**Gate**: `./mvnw -pl readings-alerting/alert-worker -am verify -Dtest='*Relay*' -Dsurefire.failIfNoSpecifiedTests=false`; with compose up, `scripts/demo.sh ingest` then `docker compose exec kafka kafka-console-consumer … --topic readings.ingested --property print.key=true --property print.headers=true --from-beginning --max-messages 1` shows key = patient_id and the `event_id` header.

## Stage 5 — Evaluator (per-event, then batched)

**References**: alert-processing.md, idempotency-and-concurrency.md (patterns 2–3), performance-and-scaling.md, BatchAlertEvaluator.java.

**Part A — per-event**: `MeasureEvaluator` strategy (BP fully; single-value evaluator for one more type to prove the strategy). `ThresholdProvider`: Caffeine L1 → Redis `MGET` → one DB query; override replaces program default; multi-program policy flag. `AlertDecisions` (pure domain): most severe band, new / fold / upgrade, reopen-on-CRITICAL flag, `is_late`, `escalate_at` from the escalation-window config. Pattern 2 transaction. Outbox topic `alerts.raised` or `alerts.raised.critical`.

**Part B — batched**: template listener (≤ 500 events / 200 ms, manual ack after commit, cooperative-sticky, bounded retries → `readings.ingested.dlq`), set-based `unnest` SQL, `DuplicateKeyException` → rollback → per-event replay. `thresholds.changed` consumer evicts L1.

**Files**: `common/domain/{Breach,Severity,ThresholdBand,EffectiveThresholds,AlertDecisions,EpisodePlan}.java`, `alert-worker/evaluator/{BloodPressureEvaluator,SingleValueEvaluator,MeasureEvaluators,BatchAlertEvaluator,ReadingsIngestedListener}.java`, `common/persistence/{JdbcAlertBatchRepository,DbThresholdProvider}.java`, `alert-worker/cache/CachingThresholdProvider.java`, `alert-worker/config/{EvaluatorKafkaConfig,AlertingProperties}.java`.

**Tests first** (testing.md: Alert idempotency, Episodes, Batch fallback):
- Unit `AlertDecisionsTest`: `most_severe_band_wins`, `patient_override_replaces_program_default`, `multi_program_takes_most_conservative`, `repeat_same_severity_folds_without_event`, `upgrade_to_critical_on_acknowledged_reopens_when_flag_on`, `upgrade_to_critical_on_acknowledged_stays_acknowledged_when_flag_off`, `reading_older_than_7_days_is_late`.
- Integration: `readings_ingested_delivered_twice_gives_one_alert_count_one_and_one_event`, `two_breaches_same_patient_measure_give_one_alert_count_two`, `high_then_critical_upgrades_severity_with_one_upgrade_event`, `critical_goes_to_critical_topic`, `batch_with_already_evaluated_reading_falls_back_and_matches_clean_run`, `poison_message_goes_to_dlq_after_n_retries`.

**Gate**: `./mvnw -pl readings-alerting/alert-worker -am verify -Dtest='*Evaluator*,*AlertDecisions*' -Dsurefire.failIfNoSpecifiedTests=false`; with compose up, `scripts/demo.sh critical` → one `alert` row, `CRITICAL`, `escalate_at` set; run again → still one row, `occurrence_cnt = 1`.

## Stage 6 — Alert state + escalation

**References**: alert-processing.md (states, SLA), idempotency-and-concurrency.md (patterns 4–5), security-phi.md (audit).

**Scope**: `acknowledge` / `resolve` as single conditional updates (`version` + legal source states); zero rows → `AlertConflict` with current state. Note required to resolve CRITICAL. Each change writes `audit_log` + `alert.state_changed` outbox row in the same transaction. Escalation scheduler (pattern 5, `SKIP LOCKED`, injected `Clock`). `ReassignmentService`: unresolved alerts move to the new primary coach and emit REASSIGNED. Service level only; HTTP in stage 8.

**Files**: `common/domain/{Alert,AlertStatus,AlertConflict}.java`, `common/persistence/{JdbcAlertStateRepository,JdbcCareAssignmentRepository}.java`, `common/alerting/{DefaultAlertService,ReassignmentService}.java`, `alert-worker/alerting/EscalationJob.java`.

**Tests first** (testing.md: Escalation, Ack vs escalation race, Optimistic lock):
- `critical_not_acknowledged_by_escalate_at_is_escalated_once_with_three_schedulers`
- `concurrent_ack_and_escalation_exactly_one_wins`
- `two_acknowledges_same_version_one_succeeds_one_conflicts_with_current_state`
- `acknowledge_clears_escalate_at_so_alert_cannot_escalate`
- `resolve_critical_without_note_is_rejected`
- `every_state_change_writes_audit_and_state_changed_event`
- `reassignment_moves_unresolved_alerts_and_emits_reassigned`

**Gate**: `./mvnw -pl readings-alerting/alert-worker -am verify -Dtest='*Escalation*,*AlertService*,*Reassign*' -Dsurefire.failIfNoSpecifiedTests=false`.

## Stage 7 — Notifier + critical lane (stretch: effectively-once delivery, demonstrated)

**References**: alert-processing.md (notifications), idempotency-and-concurrency.md (pattern 6), performance-and-scaling.md (critical lane).

**Scope**: Groups `notifier` (`alerts.raised`, `alert.state_changed`) and `notifier-critical` (`alerts.raised.critical`). Recipients from `care_assignment` at send time; channel matrix from alert-processing.md. `INSERT … ON CONFLICT (trigger_event_id, coach_id, channel) DO NOTHING`; QUEUE rows `SENT` at once. PUSH/SMS senders claim `PENDING` rows `SKIP LOCKED`, call **logging fake providers** (assignment allows persisting/logging) with `notification_id` as idempotency key; Resilience4j timeout + retry + circuit breaker; backoff via `next_attempt_at`; `FAILED` after max attempts. Reserved CRITICAL share in a simple per-provider token bucket. DIGEST: flag only (see clinical decisions).

**Files**: `alert-worker/notify/{NotificationRecorder,AlertEventsListener,CriticalAlertEventsListener,ChannelSender,LoggingPushProvider,LoggingSmsProvider,ProviderRateLimiter}.java`, `common/persistence/JdbcNotificationRepository.java`.

**Tests first** (testing.md: Notifications, Critical lane):
- `same_event_consumed_twice_gives_one_notification_per_coach_and_channel`
- `second_escalation_after_reopen_notifies_again`
- `critical_alert_notified_while_non_critical_topic_has_backlog`
- `sender_marks_sent_once_with_two_senders_racing` (the effectively-once demo)
- `provider_failure_backs_off_then_fails_after_max_attempts`

**Gate**: `./mvnw -pl readings-alerting/alert-worker -am verify -Dtest='*Notif*,*Critical*' -Dsurefire.failIfNoSpecifiedTests=false`; with compose up, `scripts/demo.sh critical` → QUEUE + PUSH rows for the primary coach; replaying the Kafka event adds none.

## Stage 8 — query-api + access + audit

**References**: api-contracts.md, security-phi.md, performance-and-scaling.md (read path).

**Scope**: Spring resource server validating JWTs from a **dev issuer** (static key from env; `scripts/mint-dev-token.sh`; tests sign their own; real OIDC described in Part B). Scopes `readings:read`, `alerts:read`, `alerts:write`, `thresholds:write`, `assignments:write`. Endpoints:
- `GET /v1/patients/{id}/readings/latest?type=` (Redis, DB fallback)
- `GET /v1/patients/{id}/readings?type=&from=&to=&cursor=&limit=` (keyset, limit ≤ 500, span ≤ 90 days)
- `GET /v1/coaches/{id}/alerts?status=&cursor=` (severity_rank, age)
- `POST /v1/alerts/{id}/acknowledge|resolve` (`If-Match` → `200` / `409` + current alert)
- `PUT /v1/patients/{id}/thresholds/{measure}` (close + insert, outbox `thresholds.changed`, Redis `DEL`)
- `POST /internal/v1/care-assignments` (service scope; applies a panel change → `ReassignmentService`)

Panel check in the controller and again by Postgres RLS (`SET LOCAL app.coach_id`). Every PHI read and every denial writes `audit_log`.

**Files**: `query-api/api/{ReadingsQueryController,CoachQueueController,AlertActionsController,ThresholdController,CareAssignmentAdminController,CursorCodec}.java`, `query-api/security/{SecurityConfig,PanelAccessGuard,RlsTransactionCustomizer}.java`, `common/persistence/{JdbcReadingQueryRepository,JdbcAlertQueueRepository,JdbcThresholdRepository}.java`, `db/migration/V…__enable_rls_policies.sql`, `scripts/mint-dev-token.sh`.

**Tests first** (testing.md: Access control, Optimistic lock over HTTP):
- `coach_cannot_read_patient_outside_panel_and_denial_is_audited`
- `rls_blocks_cross_panel_read_even_if_controller_check_is_bypassed`
- `two_acknowledges_same_if_match_one_200_one_409_with_current_alert`
- `range_query_pages_with_keyset_cursor_without_gaps_or_repeats`
- `queue_orders_by_severity_then_age`
- `threshold_change_closes_old_rows_inserts_new_and_emits_thresholds_changed`
- `latest_falls_back_to_db_when_redis_is_down`

**Gate**: `./mvnw -pl readings-alerting/query-api -am verify`; with compose up, `scripts/demo.sh queue` shows the stage-5 alert in the seeded coach's queue and a stale `If-Match` acknowledge returns `409`.

## Stage 9 — Observability: dropped (2026-11-09)

Not part of the demo. No Micrometer/Prometheus metrics, dashboards or metric tests. Stage numbers 10–13 are kept as they were. The PHI log scrubber from stage 1 stays (it is a PHI rule, not observability); its test `log_scrubber_blocks_phi_tagged_fields` moves to stage 12. Monitoring is described in Part B only.

## Stage 10 — AI feature (Part C): reading summary, stubbed LLM

**References**: security-phi.md (AI features), architecture.md (partial outages), decisions.md (D18).

**Scope**: `GET /v1/patients/{id}/summary` in query-api (panel check + audit as other PHI reads). `LlmClient` port with a **deterministic stub** only (no real provider, per your choice), plus test fakes that are slow or fail. Integration pattern: 2 s timeout, Resilience4j circuit breaker, rate limiter, daily token/cost budget counter, Redis cache `summary:{patientId}` 15 min (evicted on new alert via `alerts.raised`), rule-based fallback (`source: rules`). Prompt built from a de-identified hourly `reading_rollup` (new table, added to `schema.sql` in this stage) with IDs removed and relative times. Output labelled advisory. Nothing on the ingest or evaluation path. Rationale, failure modes and quality evaluation plan go into design.md in stage 13.

**Files**: `common/ports/{LlmClient,ReadingSummaryService}.java`, `query-api/ai/{StubLlmClient,ResilientLlmClient,DefaultReadingSummaryService,RuleBasedSummary,PromptBuilder,SummaryController}.java`, `db/migration/V…__create_reading_rollup.sql`, `alert-worker/rollup/RollupJob.java`.

**Tests first** (testing.md: AI fallback):
- `llm_timeout_returns_rule_based_summary_within_latency_budget`
- `open_circuit_skips_llm_and_returns_rules`
- `llm_down_ingest_and_alerts_unaffected`
- `prompt_contains_no_identifiers_or_free_text`
- `summary_outside_panel_is_denied_and_audited`

**Gate**: `./mvnw -pl readings-alerting/query-api -am verify -Dtest='*Summary*' -Dsurefire.failIfNoSpecifiedTests=false`; with compose up, `scripts/demo.sh summary` returns a summary, and with `VITALLINK_LLM_STUB_MODE=timeout` returns `source: rules` within ~2 s.

## Stage 11 — k6 load test

**References**: testing.md (load test), performance-and-scaling.md.

**Scope**: One k6 script with two scenarios: `burst` (200+ virtual devices, 50-reading batches, 10% deliberate retries) and `morning` (ramp shaped like the 6 AM curve, 20% breach rate). Seed SQL for load devices. Collect throughput, p50/p95/p99, duplicate rate, consumer lag, commits/s; state where the bottleneck is.

**Files**: `load/ingest.js`, `load/lib/devices.js`, `db/seed/load_devices.sql`, `load/RESULTS.md`.

**Tests first**: k6 `thresholds` encode the targets (p99, error rate) so a miss fails the run; a post-run SQL check asserts zero duplicate rows.

**Gate**: `docker compose up -d && k6 run load/ingest.js`; `load/check-duplicates.sql` returns 0; numbers recorded in `load/RESULTS.md`.

## Stage 12 — Review

**Scope**: SKILL.md review checklist over the whole repo; add `log_scrubber_blocks_phi_tagged_fields` (moved from dropped stage 9); `/code-review` and `/security-review` on the branch; map all eight guards and every `testing.md` row to a named test; fix findings test-first; dated lines in `references/decisions.md` for any decision change; list any remaining skill vs design.md conflicts.

**Gate**: `./mvnw clean verify` green; checklist table (item → test/file) with no open items.

## Stage 13 — README + Parts D and E (and Part B gaps)

**Scope**:
- **README**: what it is; one-command run; seed device keys and dev tokens; `scripts/demo.sh` (happy path + duplicate path, per Part A); how to test; load results and the bottleneck sentence; assumptions; key design decisions; clinical decisions and their flags; known gaps and stretch goals done.
- **Part D**: how I worked with AI tools (Claude Code with the project skill, plan-first gated stages, where it was overridden or corrected). You supply your own observations; I draft from the session history.
- **Part E**: onboarding "design decisions & conventions" (½–1 page, 2–3 conventions held in review, e.g. "every retried write has a DB natural key", "events only through the outbox", "API versioning and RFC 7807") and one code-review take on a deliberate corner cut (candidate: local `SKIP LOCKED` relay instead of CDC, or the stubbed LLM).
- **Part B gaps in `docs/design.md`**: a Mermaid architecture diagram in the repo (diagrams today live only in the linked design doc); a "Tradeoffs: Part A vs production" section; Part C rationale (timeouts, cost/rate limits, quality evaluation, failure modes).

**Gate**: from a fresh clone, following only the README, `docker compose up -d && scripts/demo.sh` reaches a CRITICAL alert in the coach queue and shows a `DUPLICATE`; you review Parts D/E text.

---

## Departures from the skill

**None.** Choices inside the skill's rules, noted for review:

1. Local relay is a `relay` profile of alert-worker (single replica); the skill names no host for the local poller.
2. Flyway runs as a one-shot owner-role container so apps use a least-privilege role.
3. Coach/clinician write endpoints and the internal care-assignment endpoint live in query-api; the skill assigns them to no deployable.
4. `reading_rollup` table added in stage 10 (required by performance-and-scaling.md for summaries, missing from `schema.sql`); `schema.sql` updated in the same stage.
5. Lean-scope items that remain documented instead of coded: confirmation rule and daily digest are **not implemented**, only flags defaulting to off (the skill's safe default). Setting either to `true` fails start-up with a clear message, so the controls can't look enabled when they aren't. Observability (metrics, dashboards) is not built at all (stage 9 dropped, not part of the demo); described in Part B only.

**Repo instruction conflict**: `CLAUDE.md` says `./gradlew`; you chose Maven. Stage 1 updates `CLAUDE.md` and `.claude/settings.json` to `./mvnw`.

## Open clinical decisions → config flags (safer default)

| Decision | Flag | Default |
| --- | --- | --- |
| Acknowledged alert rising to CRITICAL returns to OPEN and restarts escalation | `vitallink.alerting.reopen-acknowledged-on-critical` | `true` |
| Reject readings older than 90 days (`422 READING_TOO_OLD`) | `vitallink.ingest.reject-older-than` | `P90D` |
| Multiple programs, no override → most conservative default | `vitallink.thresholds.multi-program-policy` | `MOST_CONSERVATIVE` |
| Confirmation rule for non-critical alerts | `vitallink.alerting.confirmation.enabled` | `false` (not implemented in this cut; CRITICAL always immediate by DB CHECK) |
| Daily digest for chronically out-of-range patients | `vitallink.notify.digest.enabled` | `false` (not implemented in this cut; CRITICAL and escalations never digested) |
| Escalation window | `vitallink.alerting.escalation.default-window`, `…per-program.<programId>` | `PT15M` |
| Late-reading cutoff (assumption, not clinical-signed) | `vitallink.alerting.late-after` | `P7D` |

Each flag is named in the report of the stage that introduces it.

## Decisions taken (your answers, 2026-11-07)

| Question | Answer |
| --- | --- |
| Part C feature | Reading summary |
| Scope | Lean core + flags; stretch = effectively-once alert delivery (stage 7) |
| LLM | Stub only (with slow/failing fakes in tests) |
| Build tool | Maven (`./mvnw`) |
| Coach auth | Dev JWT issuer; Keycloak/OIDC described in Part B |
| design.md | Updated by you; skill wins on conflicts (CLAUDE.md precedence) |
| Reassignment / rebinding | Seed data + internal admin endpoint; real projection feed out of scope |
| Commits | Feature branch, commit per stage after its gate |
| Observability (2026-11-09) | Dropped: not part of the demo; stage 9 removed |

## Verification (whole plan)

- Every `testing.md` row maps to a named test in stages 3, 5, 6, 7, 8, 10; stage 12 re-checks the mapping.
- `./mvnw verify` green at every gate; from stage 5, `docker compose up -d && scripts/demo.sh` exercises ingest → dedup → persist → async alert → notification → read APIs.
- Stage 11 numbers and the stage 13 fresh-clone run are the end-to-end checks.
