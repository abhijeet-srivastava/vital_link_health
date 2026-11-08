package com.vitallink.readings.domain;

/** What the episode insert or update returns: enough to build the alerts.raised event. */
public record EpisodeRow(long alertId, long coachId, int occurrenceCnt) {
}
