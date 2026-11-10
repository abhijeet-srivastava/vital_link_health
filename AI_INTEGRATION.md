# AI integration: AI reading summary

## What it does

`GET /v1/patients/{id}/summary` (query-api) gives a coach a short plain-language summary of a
patient's last 24 hours of readings:

```json
{ "patient_id": 2, "source": "llm", "advisory": true,
  "label": "AI-generated summary. Advisory only, not a clinical assessment; alert severity comes from threshold rules.",
  "text": "Systolic stayed between 128 and 186 mmHg across 3 readings. ...",
  "window_hours": 24, "generated_at": "2026-11-09T08:00:00Z" }
```

Flow: panel check → Redis `summary:{patientId}` → on a miss, the patient's hourly rows from
`reading_rollup` (replica, row-level security) → de-identified prompt → LLM behind guards → cache
for 15 min → `ALLOWED` audit → response. Any LLM problem gives the rule-based summary instead
(`source: rules`), always with `200`.

- `reading_rollup` (new, partitioned by day, 90-day retention) holds count/min/max/mean per patient,
  measure and UTC hour. alert-worker's `RollupJob` (`scheduler` profile) recomputes the last 3 whole
  hours every 5 minutes with an idempotent upsert.
- The LLM is a port (`LlmClient`). Only a deterministic stub is wired (`StubLlmClient`;
  `VITALLINK_LLM_STUB_MODE=ok|timeout|error`); a real provider is one more adapter behind the same
  guards.
- `alerts.raised` and `alerts.raised.critical` delete the patient's cached summary (consumer group
  `query-summary-eviction`), so a new alert never sits next to a summary written before it.

## Guardrails

| Risk | Guard |
| --- | --- |
| LLM slow or down blocks something | Not on the ingest or evaluation path at all (separate deployable; a test checks those modules don't reference the LLM code). In query-api: 2 s hard timeout, circuit breaker (opens at 50% of the last 20 calls, at least 5), then rules. The rollup read finishes before the LLM call, so a slow provider never holds a DB connection |
| Cost runaway | Per-pod rate limiter (20/s, no queueing) and a per-pod daily token budget (5M tokens per UTC day); over either one, rules. Only LLM answers are cached (15 min) |
| PHI leaving the boundary | The prompt is built only from rollups: no patient, device or reading ids, no dates (only "h-3", meaning 3 hours ago), no free text (`extras` and notes never reach the rollup). Every data line has a fixed shape, `MEASURE unit \| h-N \| n= min= max= avg=`, and a test checks each line against that pattern. Logs carry the fallback reason only, never the prompt or the answer |
| Unauthorised access | Same panel check and audit as other PHI reads (`READ_SUMMARY`, `DENIED` audited before the `403`); row-level security on `reading_rollup` repeats the check in the database |
| Treated as clinical advice | `advisory: true` and a label on every response. Severity, escalation and notification never read the summary (rule 8). The instructions forbid diagnosis, treatment advice and urgency judgements. The answer is capped at 1,200 characters |
| Wrong or invented numbers | Instructions say to use only the given numbers. The rule-based text is always available as a reference |

## Cost per day at 1M patients

Assumptions (these are inputs, not measurements):

- **Prompt size.** About 110 tokens of instructions plus about 15 tokens per data line. A typical
  patient sends 2–4 BP readings a day, which makes ~9 lines, so ~250 input tokens. The worst case
  is 24 hours × 6 measures = 144 lines, about 2,300 tokens. The answer is about 150 output tokens.
- **Requests.** At most one LLM call per patient per 15 minutes, because of the cache. A realistic
  load is coaches opening about 20% of their panel a day, which is 200k summaries. The upper bound
  is every patient once a day, which is 1M.
- **Price.** A small, fast model priced at $P_in = $1 per million input tokens and $P_out = $5
  per million output tokens. These are placeholders: use the contracted price of the
  BAA-covered provider.

| Scenario | Input tokens/day | Output tokens/day | LLM cost/day |
| --- | --- | --- | --- |
| Realistic: 200k summaries, typical prompt | 50M | 30M | **≈ $200** |
| Every patient once a day, typical prompt | 250M | 150M | ≈ $1,000 |
| Every patient once a day, worst-case prompt | 2.3B | 150M | ≈ $3,050 |

The formula is `summaries × (in_tokens × P_in + out_tokens × P_out)`. The budget caps the
spend: `daily-token-budget × pods`. With 10 pods at 5M tokens that is 50M tokens a day (≈ $100–250
a day, ~110k LLM summaries); past the cap, coaches get rule-based summaries. The budget should be
set from the expected number of summaries, not left open.

Other costs:

- **Rollups.** ~5 rows per patient per day is 5M rows/day, ≈ 300 MB/day and ≈ 27 GB at 90 days.
  `RollupJob` re-aggregates 3 hours of readings (~6M rows at 50M/day) every 5 minutes, a scan of
  the current day partition. At full scale it should update rollups incrementally from
  `readings.ingested` instead.
- **Redis.** About 1 KB per cached summary. Even 1M live keys is ~1 GB.
