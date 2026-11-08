package com.vitallink.readings.domain;

/** An unresolved episode read under FOR UPDATE by the batch path, with what its events need. */
public record LockedEpisode(EpisodeKey key, ActiveEpisode episode, long coachId, int occurrenceCnt) {
}
