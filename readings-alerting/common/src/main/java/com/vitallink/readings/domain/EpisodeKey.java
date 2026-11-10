package com.vitallink.readings.domain;

/** One unresolved episode at most per key (ux_alert_active_episode). */
public record EpisodeKey(long patientId, Measure measure) {

    public static EpisodeKey of(BreachWithReading breach) {
        return new EpisodeKey(breach.patientId(), breach.measure());
    }
}
