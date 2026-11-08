package com.vitallink.readings.domain;

import java.util.UUID;

/**
 * A change a coach must hear about, as the notifier sees it: parsed from alerts.raised(.critical) or
 * alert.state_changed. eventId is the trigger_event_id of every notification it causes (guard 5): a replay reuses it,
 * a real change has a new one.
 *
 * @param coachId the alert's owner when the event was written; recipients are resolved again at record time
 */
public record AlertEvent(UUID eventId, long alertId, long patientId, long coachId, Severity severity,
                         NotificationReason reason) {
}
