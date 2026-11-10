package com.vitallink.readings.domain;

import java.time.Instant;

/** Keyset position in a coach queue: (severity_rank, created_at, alert_id) of the last row of the previous page. */
public record QueueCursor(int severityRank, Instant createdAt, long alertId) {

    public static QueueCursor after(Alert last) {
        return new QueueCursor(last.severity().rank(), last.createdAt(), last.alertId());
    }
}
