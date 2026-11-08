package com.vitallink.readings.domain;

import java.util.UUID;

/**
 * A claimed PENDING notification. notificationId is the provider idempotency key (guard 6); triggerEventId lets a
 * fallback channel be recorded under the same delivery key (guard 5).
 */
public record PendingNotification(long notificationId, UUID triggerEventId, long alertId, long coachId,
                                  Channel channel, NotificationReason reason, Severity severity, int attempts) {
}
