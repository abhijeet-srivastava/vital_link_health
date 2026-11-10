package com.vitallink.readings.domain;

import java.util.UUID;

/** One notification to record: unique on (triggerEventId, coachId, channel). */
public record NotificationRow(UUID triggerEventId, long alertId, long coachId, Channel channel,
                              NotificationReason reason, Severity severity) {
}
