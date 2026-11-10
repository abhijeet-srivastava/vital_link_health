package com.vitallink.readings.ports;

import java.time.Instant;
import java.util.List;

import com.vitallink.readings.domain.ReadingRollup;

public interface ReadingRollupRepository {

    /** Recomputes every hour from {@code since} (rounded down to the hour) from reading; returns rows changed. */
    int refresh(Instant since);

    /** The patient's rollups from the hour containing {@code since}, by measure and hour. */
    List<ReadingRollup> findSince(long patientId, Instant since);
}
