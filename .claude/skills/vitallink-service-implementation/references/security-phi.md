# Security and PHI

Readings are PHI. Design for HIPAA-style controls at the architecture level.

## Identity and access

- Devices: per-device API keys `vl_<key_id>_<secret>`; secret stored as HMAC-SHA256 with a pepper
  held in KMS; two active keys allowed during rotation; revocation evicts `dev:{keyId}`.
- Coaches: OIDC with scopes; access limited to patients in their current `care_assignment`.
  Checked in query-api and enforced again with PostgreSQL row-level security
  (`SET LOCAL app.coach_id = ...` per transaction, policy joins `care_assignment`).
- Services: OAuth2 client credentials with narrow scopes.
- A device may only post for the patient it was bound to at each reading's `measured_at`
  (`403` otherwise).

## Data protection

- TLS 1.2+ everywhere, including Kafka, Redis and the database.
- Encryption at rest with KMS-managed keys: Postgres, Kafka, Redis, S3.
- Minimum necessary: this service stores `patient_id`, not names, addresses or contact details.
- Kafka payloads, logs, traces and metric labels carry IDs and values only. A log scrubber blocks
  fields tagged as PHI; exception messages must not include request bodies.
- Archives on S3 with Object Lock; audit partitions exported daily to WORM storage.

## Audit

- Every PHI read (readings, summaries, alert details) and every alert state change writes
  `audit_log` with actor, action, outcome (`ALLOWED`/`DENIED`), patient, resource, request id.
- The service role has `INSERT` only on `audit_log`; no `UPDATE`/`DELETE`.
- Denied access is logged too - it is the signal for misuse.

## AI features

- Prompts are de-identified (no names, dates of birth, free-text notes); provider covered by a BAA.
- Output is advisory (`ai_priority`, summaries labelled as AI-generated), with timeout (2 s),
  circuit breaker, rate limit, cost budget and a rule-based fallback.
- Never on the ingest or evaluation path; never changes severity, escalation or notification.
- Sample outputs for human review to evaluate quality.

## Secrets

Environment variables or a secrets manager; nothing in the repo. Commit only example config.
