package com.vitallink.readings.query.api;

/** 428: a state change was sent without If-Match. */
public class PreconditionRequiredException extends RuntimeException {

    public PreconditionRequiredException(String message) {
        super(message);
    }
}
