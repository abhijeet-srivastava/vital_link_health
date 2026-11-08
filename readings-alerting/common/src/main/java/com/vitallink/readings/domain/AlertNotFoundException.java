package com.vitallink.readings.domain;

/** 404: no alert with this id. */
public class AlertNotFoundException extends RuntimeException {

    public AlertNotFoundException(long alertId) {
        super("No alert alert_id=" + alertId);
    }
}
