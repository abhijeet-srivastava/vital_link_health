package com.vitallink.readings.domain;

/** 422: the request as a whole is unusable (no patient_id, no readings). */
public class MalformedBatchException extends RuntimeException {
    public MalformedBatchException(String message) {
        super(message);
    }
}
