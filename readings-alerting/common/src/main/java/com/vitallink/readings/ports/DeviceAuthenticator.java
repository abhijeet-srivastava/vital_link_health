package com.vitallink.readings.ports;

import com.vitallink.readings.domain.DeviceIdentity;

import java.time.Instant;
import java.util.List;

public interface DeviceAuthenticator {
    /** Resolves "vl_<keyId>_<secret>"; constant-time HMAC compare; cached. Throws UnauthenticatedException -> 401. */
    DeviceIdentity authenticate(String apiKey);

    /**
     * The device must have been bound to {@code patientId} at every one of {@code measuredAts} (one lookup per batch).
     * Throws DeviceMismatchException -> 403 otherwise.
     */
    void assertBoundTo(DeviceIdentity device, long patientId, List<Instant> measuredAts);
}
