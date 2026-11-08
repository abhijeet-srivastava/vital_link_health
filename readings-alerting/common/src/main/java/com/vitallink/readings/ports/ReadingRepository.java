package com.vitallink.readings.ports;

import com.vitallink.readings.domain.ExistingHash;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingKey;

import java.util.List;

public interface ReadingRepository {
    /** INSERT ... ON CONFLICT DO NOTHING RETURNING the keys inserted. Rows not returned already existed. */
    List<ReadingKey> insertIgnoringDuplicates(List<Reading> batch);

    /** payload_hash of existing rows, to classify DUPLICATE vs CONFLICT. */
    List<ExistingHash> findPayloadHashes(long deviceId, List<ReadingKey> keys);
}
