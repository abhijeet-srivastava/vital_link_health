package com.vitallink.readings.domain;

/** Codes match measure.measure_code (V202611080902__insert_measures.sql). */
public enum Measure {
    SYSTOLIC(1),
    DIASTOLIC(2),
    PULSE(3),
    GLUCOSE(4),
    SPO2(5),
    WEIGHT(6);

    private final int code;

    Measure(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static Measure fromCode(int code) {
        for (Measure m : values()) {
            if (m.code == code) {
                return m;
            }
        }
        throw new IllegalArgumentException("Unknown measure_code " + code);
    }
}
