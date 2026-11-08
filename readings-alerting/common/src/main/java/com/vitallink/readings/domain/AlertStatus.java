package com.vitallink.readings.domain;

/** alert.status values (references/alert-processing.md, States). */
public enum AlertStatus {
    OPEN,
    ACKNOWLEDGED,
    ESCALATED,
    RESOLVED
}
