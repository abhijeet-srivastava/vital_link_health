package com.vitallink.readings.domain;

import java.time.Duration;

/**
 * Unpublished outbox rows: how many (capped by the caller, so the count stays cheap) and how long the oldest has
 * waited. Both near zero while the relay keeps up; they grow while Kafka or the relay is down.
 */
public record OutboxBacklog(long rows, Duration oldestAge) {
}
