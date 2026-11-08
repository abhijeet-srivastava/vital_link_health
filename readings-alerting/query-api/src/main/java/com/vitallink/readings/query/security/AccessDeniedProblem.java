package com.vitallink.readings.query.security;

/** 403: missing scope, wrong kind of caller or patient outside the coach's panel. Already audited when thrown. */
public class AccessDeniedProblem extends RuntimeException {

    public AccessDeniedProblem(String message) {
        super(message);
    }
}
