package com.vitallink.readings.ports;

import java.time.Instant;

public interface EscalationScheduler {
    /** Claims due alerts with SKIP LOCKED; escalates each once. Returns the number escalated. */
    int escalateDue(Instant now, int limit);
}
