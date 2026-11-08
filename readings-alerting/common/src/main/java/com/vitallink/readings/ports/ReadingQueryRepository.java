package com.vitallink.readings.ports;

import java.time.Instant;
import java.util.Optional;

import com.vitallink.readings.domain.Cursor;
import com.vitallink.readings.domain.Page;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;

/**
 * Read side (query-api); split from ReadingRepository so the write path has no read-replica concerns. Callers run
 * these inside a transaction that has set app.coach_id, so row-level security filters every row (second check).
 */
public interface ReadingQueryRepository {
    /** Keyset pagination on (measured_at DESC, reading_uuid), from inclusive, to exclusive. Never OFFSET. */
    Page<Reading> findRange(long patientId, ReadingType type, Instant from, Instant to, Cursor cursor, int limit);

    /** Newest reading of the type: the DB fallback when the latest-reading cache misses or Redis is down. */
    Optional<Reading> findLatest(long patientId, ReadingType type);
}
