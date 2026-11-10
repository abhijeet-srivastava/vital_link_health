package com.vitallink.readings.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * A plain-language summary of a patient's recent readings. Advisory only: it never changes severity, escalation or
 * notification (rule 8). {@code source} says whether the LLM wrote it or the rule-based fallback did.
 */
public record Summary(Source source, String text, Duration window, Instant generatedAt) {

    public enum Source {
        LLM,
        RULES
    }
}
