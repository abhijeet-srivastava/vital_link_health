package com.vitallink.readings.alerting.notify;

import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.domain.AlertEvent;
import com.vitallink.readings.domain.NotificationReason;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.messaging.AlertStateChangedV1;
import com.vitallink.readings.messaging.AlertsRaisedV1;
import com.vitallink.readings.messaging.KafkaTopics;

/** Record value -> {@link AlertEvent}; throws on anything that is not a v1 payload of the topic's type. */
final class AlertEvents {

    private AlertEvents() {
    }

    /** Empty for changes nobody is notified about (ACKNOWLEDGED, RESOLVED). */
    static Optional<AlertEvent> parse(ObjectMapper json, String topic, String value) throws JsonProcessingException {
        if (value == null) {
            throw new IllegalArgumentException("Record without a value on " + topic);
        }
        return switch (topic) {
            case KafkaTopics.ALERTS_RAISED, KafkaTopics.ALERTS_RAISED_CRITICAL -> Optional.of(raised(json, value));
            case KafkaTopics.ALERT_STATE_CHANGED -> stateChanged(json, value);
            default -> throw new IllegalArgumentException("Unexpected topic " + topic);
        };
    }

    private static AlertEvent raised(ObjectMapper json, String value) throws JsonProcessingException {
        AlertsRaisedV1 e = json.readValue(value, AlertsRaisedV1.class);
        if (e.schemaVersion() != AlertsRaisedV1.SCHEMA_VERSION || e.eventId() == null) {
            throw new IllegalArgumentException("Unsupported alerts.raised schema_version=" + e.schemaVersion());
        }
        return new AlertEvent(e.eventId(), e.alertId(), e.patientId(), e.coachId(), Severity.valueOf(e.severity()),
                NotificationReason.valueOf(e.reason()));
    }

    private static Optional<AlertEvent> stateChanged(ObjectMapper json, String value) throws JsonProcessingException {
        AlertStateChangedV1 e = json.readValue(value, AlertStateChangedV1.class);
        if (e.schemaVersion() != AlertStateChangedV1.SCHEMA_VERSION || e.eventId() == null) {
            throw new IllegalArgumentException("Unsupported alert.state_changed schema_version=" + e.schemaVersion());
        }
        NotificationReason reason = switch (e.reason()) {
            case "ESCALATED" -> NotificationReason.ESCALATED;
            case "REASSIGNED" -> NotificationReason.REASSIGNED;
            case "ACKNOWLEDGED", "RESOLVED" -> null;
            default -> throw new IllegalArgumentException("Unknown alert.state_changed reason");
        };
        return Optional.ofNullable(reason).map(r -> new AlertEvent(e.eventId(), e.alertId(), e.patientId(),
                e.coachId(), Severity.valueOf(e.severity()), r));
    }
}
