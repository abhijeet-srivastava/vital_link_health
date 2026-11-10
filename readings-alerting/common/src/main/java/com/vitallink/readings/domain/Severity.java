package com.vitallink.readings.domain;

/** Matches the severity CHECK and alert.severity_rank in references/schema.sql: rank 1 is the most severe. */
public enum Severity {
    CRITICAL(1),
    HIGH(2),
    MEDIUM(3);

    private final int rank;

    Severity(int rank) {
        this.rank = rank;
    }

    public int rank() {
        return rank;
    }

    public boolean moreSevereThan(Severity other) {
        return rank < other.rank;
    }
}
