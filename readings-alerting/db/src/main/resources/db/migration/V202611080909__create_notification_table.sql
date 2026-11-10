CREATE TABLE notification (
  notification_id  BIGSERIAL PRIMARY KEY,
  trigger_event_id UUID NOT NULL,          -- the alerts.raised / alert.state_changed event that caused it
  alert_id         BIGINT NOT NULL REFERENCES alert,
  coach_id         BIGINT NOT NULL,
  channel          TEXT NOT NULL CHECK (channel IN ('QUEUE','PUSH','SMS','DIGEST')),
  reason           TEXT NOT NULL CHECK (reason IN ('RAISED','SEVERITY_UPGRADED','ESCALATED','REASSIGNED')),
  severity         TEXT NOT NULL,          -- severity at the time; shown in the message
  status           TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','SENT','FAILED')),
  attempts         INT NOT NULL DEFAULT 0,
  next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at          TIMESTAMPTZ,
  UNIQUE (trigger_event_id, coach_id, channel)                 -- delivery idempotency: one per change, coach, channel
);
CREATE INDEX ix_notification_alert ON notification (alert_id);
CREATE INDEX ix_notification_due ON notification (channel, next_attempt_at) WHERE status = 'PENDING';
