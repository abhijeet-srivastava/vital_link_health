package com.vitallink.readings.domain;

/** 409: the version or source state did not match. Carries the current alert so the client can redisplay it. */
public class AlertConflict extends RuntimeException {

    private final transient Alert current;

    public AlertConflict(Alert current) {
        super("Alert changed alert_id=" + current.alertId() + " status=" + current.status() + " version="
                + current.version());
        this.current = current;
    }

    public Alert current() {
        return current;
    }
}
