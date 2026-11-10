package com.vitallink.readings.domain;

// Values match the notification.channel CHECK in references/schema.sql.
public enum Channel {
    QUEUE,
    PUSH,
    SMS,
    DIGEST
}
