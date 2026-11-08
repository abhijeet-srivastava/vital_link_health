package com.vitallink.readings.domain;

/** Codes match measure.reading_type and reading.reading_type in references/schema.sql. */
public enum ReadingType {
    BLOOD_PRESSURE(1),
    GLUCOSE(2),
    PULSE_OX(3),
    WEIGHT(4);

    private final int code;

    ReadingType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static ReadingType fromCode(int code) {
        for (ReadingType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new IllegalArgumentException("Unknown reading_type " + code);
    }
}
