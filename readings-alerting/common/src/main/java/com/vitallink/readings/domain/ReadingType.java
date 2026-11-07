package com.vitallink.readings.domain;

// Codes match measure.reading_type in references/schema.sql; behaviour is added in stage 3.
public enum ReadingType {
    BLOOD_PRESSURE,
    GLUCOSE,
    PULSE_OX,
    WEIGHT
}
