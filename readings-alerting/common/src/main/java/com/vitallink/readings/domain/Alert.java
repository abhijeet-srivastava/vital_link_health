package com.vitallink.readings.domain;

import java.time.Instant;

/**
 * One alert episode as a coach sees it. {@code resolution_note} is free text (PHI) and is deliberately not carried,
 * so it cannot reach toString, logs or a 409 body by accident.
 */
public record Alert(long alertId, long patientId, Measure measure, long coachId, Severity severity,
                    AlertStatus status, boolean late, Instant escalateAt, Instant firstSeenAt, Instant lastSeenAt,
                    int occurrenceCnt, int version, Instant createdAt, Long acknowledgedBy, Instant acknowledgedAt,
                    Long resolvedBy, Instant resolvedAt) {
}
