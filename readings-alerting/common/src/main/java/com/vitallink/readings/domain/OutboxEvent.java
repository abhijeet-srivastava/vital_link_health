package com.vitallink.readings.domain;

import java.util.UUID;

/** An outbox_event row. payloadJson carries IDs and values only (rule 9). */
public record OutboxEvent(UUID eventId, String topic, String eventType, String aggregateKey, int schemaVersion,
                          String payloadJson) {
}
