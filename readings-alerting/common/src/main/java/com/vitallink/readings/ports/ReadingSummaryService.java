package com.vitallink.readings.ports;

import com.vitallink.readings.domain.Summary;

import java.time.Duration;

public interface ReadingSummaryService {
    /** LLM summary from de-identified rollups; falls back to a rule-based summary on timeout or open circuit. */
    Summary summarize(long patientId, Duration window);
}
