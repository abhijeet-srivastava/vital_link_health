package com.vitallink.readings.domain;

import java.time.Instant;

/** Active device_credential row of an active device. secretHash = HMAC-SHA256(secret, pepper). */
public record DeviceCredential(String keyId, long deviceId, byte[] secretHash, Instant expiresAt) {

    public boolean expiredAt(Instant now) {
        return expiresAt != null && !expiresAt.isAfter(now);
    }

    @Override
    public String toString() {
        return "DeviceCredential[keyId=" + keyId + ", deviceId=" + deviceId + "]";
    }
}
