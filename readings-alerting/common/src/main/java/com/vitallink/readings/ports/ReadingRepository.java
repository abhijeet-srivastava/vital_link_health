package com.vitallink.readings.ports;

import com.vitallink.readings.domain.Cursor;
import com.vitallink.readings.domain.ExistingHash;
import com.vitallink.readings.domain.Page;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingKey;
import com.vitallink.readings.domain.ReadingType;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface ReadingRepository {
    /** INSERT ... ON CONFLICT DO NOTHING RETURNING reading_uuid. Rows not returned already existed. */
    List<UUID> insertIgnoringDuplicates(List<Reading> batch);

    /** payload_hash of existing rows, to classify DUPLICATE vs CONFLICT. */
    List<ExistingHash> findPayloadHashes(long deviceId, List<ReadingKey> keys);

    /** Keyset pagination on (measured_at DESC, reading_uuid). Never OFFSET. */
    Page<Reading> findRange(long patientId, ReadingType type, Instant from, Instant to, Cursor cursor, int limit);
}
