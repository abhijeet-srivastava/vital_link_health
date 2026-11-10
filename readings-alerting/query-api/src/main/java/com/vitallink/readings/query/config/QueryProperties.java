package com.vitallink.readings.query.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code vitallink.query.*}; defaults live in application.yml.
 *
 * @param latestTtl       TTL of latest:{patient}:{type} when a DB fallback backfills it
 * @param maxRange        widest from..to span of a range query
 * @param maxPageSize     largest {@code limit} of a range query
 * @param defaultPageSize range page size without {@code limit}
 * @param queuePageSize   coach queue page size
 */
@ConfigurationProperties("vitallink.query")
public record QueryProperties(Duration latestTtl, Duration maxRange, int maxPageSize, int defaultPageSize,
                              int queuePageSize) {
}
