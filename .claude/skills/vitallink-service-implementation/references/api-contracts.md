# API contracts

All endpoints live under `/v1`. A breaking change means `/v2` alongside `/v1`; never change the
meaning of an existing field. Errors use RFC 7807 `application/problem+json` with a stable `type`
URI and a machine-readable `code`.

## Authentication

| Caller | Scheme | Notes |
| --- | --- | --- |
| Device | `X-Device-Key: vl_<key_id>_<secret>` | `key_id` looks up the row; secret checked by constant-time HMAC compare; cached 5 min. mTLS is a later upgrade. |
| Care coach | OIDC bearer token | Scopes `readings:read`, `alerts:read`, `alerts:write`; patient access limited to the coach's current `care_assignment` |
| Clinician / admin | OIDC bearer token | `thresholds:write`, `policy:write` |
| Service-to-service | OAuth2 client credentials | Narrow scopes per service; `assignments:write` for `POST /internal/v1/care-assignments` |

Tokens carry `sub` (coach, clinician or service id; numeric for coaches), `vl_actor`
(`COACH`/`CLINICIAN`/`SERVICE`) and `scope`. Locally an HS256 dev issuer (`scripts/mint-dev-token.sh`);
production uses the OIDC issuer's JWKS. Patient reads are coach-only for now (service reads need a pool
outside the coach policy; not wired yet).

## Endpoints

| Method & path | Caller | Purpose | Success |
| --- | --- | --- | --- |
| `POST /v1/devices/{deviceId}/readings` | Device | Ingest 1-500 readings | `202` + per-item status |
| `GET /v1/patients/{patientId}/readings/latest?type=` | Coach, service | Latest reading per type (Redis, DB fallback) | `200` |
| `GET /v1/patients/{patientId}/readings?type=&from=&to=&cursor=&limit=` | Coach, service | Time range, keyset pagination (limit ≤ 500) | `200` + `next_cursor` |
| `GET /v1/coaches/{coachId}/alerts?status=&cursor=` | Coach | Queue: unresolved, ordered by `severity_rank`, then age | `200` |
| `POST /v1/alerts/{alertId}/acknowledge` | Coach | `If-Match: <version>` required | `200` or `409` |
| `POST /v1/alerts/{alertId}/resolve` | Coach | `If-Match`; `note` required for CRITICAL | `200` or `409` |
| `GET /v1/patients/{patientId}/summary` | Coach | AI summary, cached 15 min | `200` (`source: llm` or `source: rules`) |
| `PUT /v1/patients/{patientId}/thresholds/{measure}` | Clinician | Replace bands (closes old rows, inserts new) | `200` |
| `PUT /v1/patients/{patientId}/notification-policy` | Clinician | `IMMEDIATE` or `DAILY_DIGEST` for non-critical | `200` |
| `POST /internal/v1/care-assignments` | Service | Make `coach_id` the patient's PRIMARY; unresolved alerts move to it | `200` + `moved_alerts` |

Queue items carry `escalateAt`, the escalation *deadline*: set only while the alert is `OPEN` and `CRITICAL`,
and `null` once it is acknowledged, escalated or resolved (schema CHECK on `escalate_at`). An `ESCALATED` alert
therefore shows `escalateAt: null`; the time it escalated is the `audit_log` row with action `ESCALATE_ALERT`.

## Ingest

Request (a single reading is a batch of one):

```json
{
  "patient_id": 8812,
  "readings": [
    {
      "reading_id": "7c1e0b7a-2f7e-4c1d-9a51-1d2b3c4d5e6f",
      "device_seq": 10422,
      "type": "BLOOD_PRESSURE",
      "measured_at": "2026-11-07T06:42:10Z",
      "values": { "systolic": 186, "diastolic": 94, "pulse": 78 },
      "unit": "mmHg"
    }
  ]
}
```

Response `202`:

```json
{
  "results": [
    { "reading_id": "7c1e0b7a-...", "status": "ACCEPTED" }
  ]
}
```

Per-item `status`: `ACCEPTED` (new), `DUPLICATE` (same id, same values), `CONFLICT` (same id,
different values - logged as an integrity event), `REJECTED` (with `code`, e.g.
`IMPLAUSIBLE_VALUE`, `READING_TOO_OLD`, `FUTURE_TIMESTAMP`). A batch with some rejected items still
returns `202` for the rest.

Request-level errors: `401` bad key · `403` device not bound to that patient at `measured_at` ·
`413` more than 500 readings · `422` malformed body · `429` rate limited (`Retry-After`) · `503`
backpressure (`Retry-After`, jittered).

## Pagination

Cursor = opaque base64 of `(measured_at, reading_uuid)` for readings and
`(severity_rank, created_at, alert_id)` for the queue. Never `OFFSET`. Range queries require both
`from` and `to`, max span 90 days (older data comes from the archive service).

## Concurrency on state changes

`If-Match` carries the alert `version` (`"3"`, `W/"3"` or `3`); without it `428 IF_MATCH_REQUIRED`.
`200` returns the alert with `ETag: "<new version>"`. A mismatch returns `409 ALERT_CONFLICT` with the
current alert in `current` (and its `ETag`) so the client can redisplay without another round trip.
Resolving a CRITICAL alert without `note` is `422 RESOLUTION_NOTE_REQUIRED`.

`PUT .../thresholds/{measure}` body: `{"bands": [{"operator": "GT", "limit": 160, "severity": "HIGH"}]}`
(one band per operator and severity; limits inside the measure's plausibility bounds; `[]` removes the
override). Replaying the current bands is a no-op (`changed: false`, no event). A concurrent replacement
loses with `409 THRESHOLD_CONFLICT`.

## Access and audit

Coach endpoints check scope and the coach's current `care_assignment` (PRIMARY or BACKUP) in the
controller; refusals are `403 ACCESS_DENIED` and write a `DENIED` audit row with the patient concerned.
Allowed PHI reads write an `ALLOWED` row after the read and before the response (a queue page: one row
per patient shown). Row-level security enforces the panel again in the database.
