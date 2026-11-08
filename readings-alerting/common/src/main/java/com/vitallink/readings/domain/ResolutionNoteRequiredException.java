package com.vitallink.readings.domain;

/** 422: a CRITICAL alert is resolved only with a note. The message carries the ID only, never the note. */
public class ResolutionNoteRequiredException extends RuntimeException {

    public ResolutionNoteRequiredException(long alertId) {
        super("Resolution note required for CRITICAL alert_id=" + alertId);
    }
}
