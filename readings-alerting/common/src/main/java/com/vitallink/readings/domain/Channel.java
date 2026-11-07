package com.vitallink.readings.domain;

// Values match the notification.channel CHECK in references/schema.sql; senders arrive in stage 7.
public enum Channel {
    QUEUE,
    PUSH,
    SMS,
    DIGEST
}
