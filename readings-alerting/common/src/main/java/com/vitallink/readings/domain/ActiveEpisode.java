package com.vitallink.readings.domain;

/** The unresolved alert for one (patient, measure), read under FOR UPDATE (guard 3). */
public record ActiveEpisode(long alertId, AlertStatus status, Severity severity, boolean late) {
}
