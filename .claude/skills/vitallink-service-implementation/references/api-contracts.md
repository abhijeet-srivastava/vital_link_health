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
| Service-to-service | OAuth2 client credentials | Narrow scopes per service |

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

`If-Match` carries the alert `version`. A mismatch returns `409` with the current alert in the
body so the client can redisplay without another round trip.
