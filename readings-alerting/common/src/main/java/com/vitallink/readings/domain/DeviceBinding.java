package com.vitallink.readings.domain;

import java.time.Instant;

/** device_binding row: the device belonged to patientId over [validFrom, validTo); validTo null = current. */
public record DeviceBinding(long patientId, Instant validFrom, Instant validTo) {

    public boolean covers(Instant t) {
        return !t.isBefore(validFrom) && (validTo == null || t.isBefore(validTo));
    }
}
