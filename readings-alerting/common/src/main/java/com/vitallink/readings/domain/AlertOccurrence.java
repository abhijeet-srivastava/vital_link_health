package com.vitallink.readings.domain;

/** An alert_occurrence row: the breach folded into alertId. Its PK is guard 4. */
public record AlertOccurrence(long alertId, BreachWithReading breach) {
}
