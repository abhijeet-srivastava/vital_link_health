package com.vitallink.readings.ingest.backpressure;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code vitallink.ingest.backpressure.*}; defaults live in application.yml.
 *
 * @param maxInFlight             ingest requests handled at once per pod; more is 503 IN_FLIGHT
 * @param maxOutboxAge            oldest unpublished outbox row older than this is 503 OUTBOX_BACKLOG
 * @param maxOutboxRows           more unpublished outbox rows than this is 503 OUTBOX_BACKLOG
 * @param outboxSampleInterval    how often the outbox backlog is sampled (requests read the last sample)
 * @param retryAfterMin           503 Retry-After is drawn uniformly from [min, max] so devices don't retry in step
 * @param retryAfterMax           see retryAfterMin
 * @param deviceBurst             per-device token bucket size (requests)
 * @param deviceRequestsPerMinute per-device refill rate; beyond the bucket a request is 429
 */
@ConfigurationProperties("vitallink.ingest.backpressure")
public record BackpressureProperties(int maxInFlight, Duration maxOutboxAge, long maxOutboxRows,
                                     Duration outboxSampleInterval, Duration retryAfterMin, Duration retryAfterMax,
                                     int deviceBurst, int deviceRequestsPerMinute) {
}
