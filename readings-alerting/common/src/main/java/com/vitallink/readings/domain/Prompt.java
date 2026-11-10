package com.vitallink.readings.domain;

/**
 * A de-identified LLM prompt: fixed instructions plus closed-form data lines (measure, unit, hours ago, count, min, max,
 * mean). Built only from {@link ReadingRollup}, so it cannot carry patient, device or reading ids, dates or free text.
 */
public record Prompt(String instructions, String data) {

    /** Rough token count (about four characters per token) for rate and budget accounting. */
    public int estimatedTokens() {
        return (instructions.length() + data.length()) / 4 + 1;
    }
}
