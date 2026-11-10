package com.vitallink.readings.domain;

public enum RejectionCode {
    MALFORMED_READING,
    UNSUPPORTED_TYPE,
    UNSUPPORTED_UNIT,
    IMPLAUSIBLE_VALUE,
    FUTURE_TIMESTAMP,
    READING_TOO_OLD
}
