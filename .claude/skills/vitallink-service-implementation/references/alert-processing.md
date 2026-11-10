# Alert processing

An alert passes through three transactions: evaluate a reading into an episode, change the
episode's state, and turn each change a coach must see into a notification.

## Severity evaluation

1. Split the reading into measures with `reading_type_measure` (never `measure.reading_type`, which
   allows one device type per measure): a BP row yields SYSTOLIC, DIASTOLIC and PULSE; a pulse-ox
   row yields SPO2 from `value_num` and PULSE from `pulse` when present; glucose and weight use
   `value_num`. PULSE thresholds are the same whichever device measured it (open clinical decision).
2. Effective bands for (patient, measure): patient overrides from `threshold_rule` replace program
   defaults for that measure. With several enrolled programs and no override, evaluate every
   program default and take the most severe result (open clinical decision; keep configurable).
3. Each band is one rule: `GT limit` or `LT limit` with a severity. A value may cross several bands
   (`GT 160 HIGH`, `GT 180 CRITICAL`); the result is the **most severe** band crossed. No band
   crossed = no alert, and nothing is resolved automatically.
4. Severity is authoritative and rule-derived. `ai_priority` is advisory and only affects ordering
   hints in the UI, never severity, escalation or notification.
5. Confirmation rule (non-CRITICAL only, off by default): open the episode only once `confirm_n`
   breaches fall inside `confirm_window`, counted in Redis `confirm:{patient}:{measure}`. If Redis
   is unavailable, alert immediately (fail safe).
6. A reading with `measured_at` older than 7 days still alerts, with `is_late = true` and lower
   queue priority. Older than 90 days is rejected at ingest.

## Episodes

- `alert` = one episode per (patient, measure) while not `RESOLVED`
  (`ux_alert_active_episode`).
- A repeat breach at the same or lower severity: `occurrence_cnt + 1`, `last_seen_at` moved, no
  event, no notification. Only unindexed columns change → HOT update.
- A higher severity: severity upgraded in place, `SEVERITY_UPGRADED` event.
- Every breaching reading writes `alert_occurrence` with the exact `threshold_id` (audit trail and
  idempotency key).
- After `RESOLVED`, a new breach opens a new alert; resolved alerts are never reopened.

## States

| Transition | Trigger | Guard in the `UPDATE` | Side effects |
| --- | --- | --- | --- |
| — → `OPEN` | First breach (evaluator) | Unique active episode | `escalate_at` set if CRITICAL; `alerts.raised` (RAISED) |
| `OPEN` → `ACKNOWLEDGED` | Coach | `version` match, `status IN ('OPEN','ESCALATED')` | Clears `escalate_at`; audit; `alert.state_changed` |
| `OPEN` → `ESCALATED` | Scheduler at `escalate_at` | `status = 'OPEN'`, `SKIP LOCKED` | Clears `escalate_at`; `alert.state_changed` (ESCALATED) |
| `ESCALATED` → `ACKNOWLEDGED` | Primary or backup coach | `version` match | Audit; `alert.state_changed` |
| `ACKNOWLEDGED` → `OPEN` | Severity rises to CRITICAL (evaluator) | Episode row lock | Sets `escalate_at`; `alerts.raised` (SEVERITY_UPGRADED) - open clinical decision |
| any unresolved → `RESOLVED` | Coach; note required for CRITICAL | `version` match | Clears `escalate_at`; audit |
| any unresolved, new `coach_id` | Panel change in `care_assignment` | `status <> 'RESOLVED'` | `alert.state_changed` (REASSIGNED) |

`CHECK (escalate_at IS NULL OR (status = 'OPEN' AND severity = 'CRITICAL'))` makes the database
reject any transition that forgets to clear or set the timer correctly.

## Changes into notifications

| Change | Reason | To | Channels |
| --- | --- | --- | --- |
| New episode | `RAISED` | Primary coach | QUEUE; + PUSH if CRITICAL |
| Severity raised | `SEVERITY_UPGRADED` | Primary coach | QUEUE; + PUSH if CRITICAL |
| Escalated | `ESCALATED` | Backup coach and primary | QUEUE, PUSH, SMS |
| Reassigned | `REASSIGNED` | New coach | QUEUE |
| Non-critical with `notification_policy = DAILY_DIGEST` | as above | Primary coach | DIGEST (one daily summary) |
| Acknowledged, resolved, repeat breach | none | — | Audit and queue update only |

Rules:

- One `notification` row per (trigger event, coach, channel). A replay reuses the event id and is
  ignored; each real change has a new event id and notifies again.
- QUEUE rows are `SENT` immediately - the queue reads the `alert` table.
- CRITICAL alerts and escalations are never digested, never held by the confirmation rule, and
  ride the critical lane: CRITICAL alerts on `alerts.raised.critical`, escalations on
  `alert.state_changed`. Both topics are consumed by the `notifier-critical` group, each in its own
  container, with a reserved provider quota (D32).
- Recipients come from `care_assignment` at send time (PRIMARY, plus BACKUP for escalation).
- PUSH falls back to SMS while the push provider's circuit is open: an SMS row is recorded for the same
  change and coach (same delivery key, so at most one, and none when the change already pages by SMS).
  The PUSH row stays `PENDING` and is still delivered when the circuit closes (D33).

## Escalation SLA

- 15 minutes from the moment an alert becomes OPEN + CRITICAL (not from `created_at`).
- Make the duration configurable per program; keep 15 minutes as the default.
