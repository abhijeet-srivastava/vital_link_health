package com.vitallink.readings.query.security;

/** OAuth2 scopes from references/api-contracts.md, plus the service scope for panel changes. */
public final class Scopes {

    public static final String READINGS_READ = "readings:read";
    public static final String ALERTS_READ = "alerts:read";
    public static final String ALERTS_WRITE = "alerts:write";
    public static final String THRESHOLDS_WRITE = "thresholds:write";
    public static final String ASSIGNMENTS_WRITE = "assignments:write";

    private Scopes() {
    }
}
