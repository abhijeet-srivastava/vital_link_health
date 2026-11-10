package com.vitallink.readings.ports;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.vitallink.readings.domain.AlertOccurrence;
import com.vitallink.readings.domain.BreachWithReading;
import com.vitallink.readings.domain.EpisodeGroupPlan;
import com.vitallink.readings.domain.EpisodeKey;
import com.vitallink.readings.domain.EpisodeRow;
import com.vitallink.readings.domain.EpisodeUpdate;
import com.vitallink.readings.domain.LockedEpisode;

/**
 * Batched evaluator writes (pattern 3): one set-based statement per step, all inside the caller's transaction. Same
 * guards as {@link AlertEpisodeRepository}; any unique violation propagates and rolls the whole batch back.
 */
public interface AlertBatchRepository {

    /** Guard 4 pre-check: the breaches with no alert_occurrence yet, same instances, same order. */
    List<BreachWithReading> withoutExistingOccurrences(List<BreachWithReading> breaches);

    /** Guard 3: every unresolved episode for these keys, locked FOR UPDATE until commit. */
    Map<EpisodeKey, LockedEpisode> lockActiveEpisodes(Collection<EpisodeKey> keys);

    /**
     * New episodes, each owned by its patient's current PRIMARY coach; NoPrimaryCoachException when a patient has
     * none. A concurrently opened episode throws DuplicateKeyException (ux_alert_active_episode).
     */
    Map<EpisodeKey, EpisodeRow> insertEpisodes(List<EpisodeGroupPlan> plans);

    /** Folds into existing episodes; repeat-only folds touch unindexed columns only (HOT). */
    void updateEpisodes(List<EpisodeUpdate> updates);

    /** No ON CONFLICT: a duplicate throws DuplicateKeyException and the batch rolls back (guard 4). */
    void insertOccurrences(List<AlertOccurrence> occurrences);
}
