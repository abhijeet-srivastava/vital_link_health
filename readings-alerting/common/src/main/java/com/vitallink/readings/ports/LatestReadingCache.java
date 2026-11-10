package com.vitallink.readings.ports;

import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingType;

import java.util.Optional;

public interface LatestReadingCache {
    /** Lua compare-and-set on measured_at; best effort, never throws to the caller. */
    void putIfNewer(long patientId, ReadingType type, Reading reading);

    Optional<Reading> get(long patientId, ReadingType type);
}
