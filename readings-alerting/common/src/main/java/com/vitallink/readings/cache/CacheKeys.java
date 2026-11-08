package com.vitallink.readings.cache;

import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.ReadingType;

/** Redis key names from references/architecture.md, shared by the apps that write, read or evict them. */
public final class CacheKeys {

    private CacheKeys() {
    }

    /** Latest reading per patient and type: ingest writes (Lua CAS), query-api reads and backfills. */
    public static String latest(long patientId, ReadingType type) {
        return "latest:" + patientId + ":" + type.name();
    }

    /** Effective threshold bands: the evaluator caches them, the threshold API deletes them on change. */
    public static String thresholds(long patientId, Measure measure) {
        return "thr:" + patientId + ":" + measure.code();
    }
}
