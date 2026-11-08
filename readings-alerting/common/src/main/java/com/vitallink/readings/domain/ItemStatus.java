package com.vitallink.readings.domain;

public enum ItemStatus {
    /** New row stored and announced. */
    ACCEPTED,
    /** Same id, same values: already stored, nothing changed. */
    DUPLICATE,
    /** Same id, different values: original kept, logged as an integrity event. */
    CONFLICT,
    /** Not stored; see {@link RejectionCode}. */
    REJECTED
}
