package com.vitallink.readings.messaging;

/** Kafka headers on every published event. event_id lets consumers dedupe a re-published row (guard 2). */
public final class EventHeaders {

    public static final String EVENT_ID = "event_id";
    public static final String EVENT_TYPE = "event_type";
    public static final String SCHEMA_VERSION = "schema_version";

    private EventHeaders() {
    }
}
