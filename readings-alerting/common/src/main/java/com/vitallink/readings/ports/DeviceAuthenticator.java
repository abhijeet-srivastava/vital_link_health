package com.vitallink.readings.ports;

import com.vitallink.readings.domain.DeviceIdentity;

import java.time.Instant;

public interface DeviceAuthenticator {
    /** Resolves "vl_<keyId>_<secret>"; constant-time HMAC compare; cached. Throws UnauthenticatedException -> 401. */
    DeviceIdentity authenticate(String apiKey);

    /** Patient the device was bound to at {@code measuredAt}; throws DeviceMismatchException -> 403 if not {@code patientId}. */
    void assertBoundTo(DeviceIdentity device, long patientId, Instant measuredAt);
}
