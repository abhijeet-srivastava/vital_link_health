package com.vitallink.readings.domain;

/** A batch fold into an existing episode: one UPDATE row however many breaches the plan holds. */
public record EpisodeUpdate(long alertId, EpisodeGroupPlan plan) {
}
