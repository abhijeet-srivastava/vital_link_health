package com.vitallink.readings.ports;

import java.util.Optional;

import com.vitallink.readings.domain.ActiveEpisode;
import com.vitallink.readings.domain.BreachWithReading;
import com.vitallink.readings.domain.EpisodePlan;
import com.vitallink.readings.domain.EpisodeRow;
import com.vitallink.readings.domain.Measure;

/** Per-event evaluator writes (pattern 2). Every method runs inside the caller's transaction. */
public interface AlertEpisodeRepository {

    /** Guard 4 pre-check: this breach was already evaluated. The occurrence PK is the guarantee. */
    boolean occurrenceExists(BreachWithReading breach);

    /** Guard 3: the unresolved episode for (patient, measure), locked FOR UPDATE until commit. */
    Optional<ActiveEpisode> lockActiveEpisode(long patientId, Measure measure);

    /** New episode owned by the current PRIMARY coach; NoPrimaryCoachException when there is none. */
    EpisodeRow openEpisode(BreachWithReading breach, EpisodePlan.Open plan);

    EpisodeRow foldEpisode(BreachWithReading breach, EpisodePlan.Fold plan);

    /** No ON CONFLICT: a duplicate throws DuplicateKeyException and the whole transaction rolls back (guard 4). */
    void insertOccurrence(long alertId, BreachWithReading breach);
}
