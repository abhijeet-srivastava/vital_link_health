package com.vitallink.readings.ingest.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code vitallink.ingest.*}; defaults live in application.yml.
 *
 * @param maxBatchSize    readings per request; more is 413
 * @param maxFutureSkew   measured_at later than now + skew is FUTURE_TIMESTAMP
 * @param rejectOlderThan open clinical decision: measured_at older than this is READING_TOO_OLD; blank disables
 * @param latestTtl       TTL of latest:{patient}:{type} in Redis
 */
@ConfigurationProperties("vitallink.ingest")
public record IngestProperties(int maxBatchSize, Duration maxFutureSkew, Duration rejectOlderThan, Duration latestTtl) {

    public IngestProperties {
        if (rejectOlderThan != null && (rejectOlderThan.isZero() || rejectOlderThan.isNegative())) {
            rejectOlderThan = null;
        }
    }
}
