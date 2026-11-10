package com.vitallink.readings.ports;

import com.vitallink.readings.domain.DeviceBinding;
import com.vitallink.readings.domain.DeviceCredential;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface DeviceDirectory {
    /** Active credential of an active device, by the public key_id. Safe to cache (revocation latency = TTL). */
    Optional<DeviceCredential> findActiveCredential(String keyId);

    /** Bindings of the device overlapping [from, to], ordered by valid_from. Never cached: rebinding must be immediate. */
    List<DeviceBinding> findBindings(long deviceId, Instant from, Instant to);
}
