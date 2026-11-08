package com.vitallink.readings.ports;

import com.vitallink.readings.domain.Cursor;
import com.vitallink.readings.domain.Page;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;

import java.time.Instant;

/** Read side (query-api, stage 8); split from ReadingRepository so the write path has no read-replica concerns. */
public interface ReadingQueryRepository {
    /** Keyset pagination on (measured_at DESC, reading_uuid). Never OFFSET. */
    Page<Reading> findRange(long patientId, ReadingType type, Instant from, Instant to, Cursor cursor, int limit);
}
