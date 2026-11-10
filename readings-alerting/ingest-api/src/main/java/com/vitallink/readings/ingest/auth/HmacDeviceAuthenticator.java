package com.vitallink.readings.ingest.auth;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vitallink.readings.domain.DeviceBinding;
import com.vitallink.readings.domain.DeviceCredential;
import com.vitallink.readings.domain.DeviceIdentity;
import com.vitallink.readings.domain.DeviceKeys;
import com.vitallink.readings.domain.DeviceMismatchException;
import com.vitallink.readings.domain.UnauthenticatedException;
import com.vitallink.readings.ports.DeviceAuthenticator;
import com.vitallink.readings.ports.DeviceDirectory;

public class HmacDeviceAuthenticator implements DeviceAuthenticator {

    private static final Logger log = LoggerFactory.getLogger(HmacDeviceAuthenticator.class);

    /** Compared against when the key_id is unknown, so the response time does not reveal which key_ids exist. */
    private static final byte[] NO_SUCH_KEY = new byte[32];

    private final DeviceDirectory directory;
    private final byte[] pepper;
    private final Clock clock;

    public HmacDeviceAuthenticator(DeviceDirectory directory, byte[] pepper, Clock clock) {
        this.directory = directory;
        this.pepper = pepper.clone();
        this.clock = clock;
    }

    @Override
    public DeviceIdentity authenticate(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new UnauthenticatedException("Missing X-Device-Key");
        }
        DeviceKeys.ApiKey key = DeviceKeys.parse(apiKey)
                .orElseThrow(() -> new UnauthenticatedException("Malformed X-Device-Key"));
        Optional<DeviceCredential> credential = directory.findActiveCredential(key.keyId());
        boolean secretMatches = DeviceKeys.matches(key.secret(), pepper,
                credential.map(DeviceCredential::secretHash).orElse(NO_SUCH_KEY));
        if (credential.isEmpty() || !secretMatches || credential.get().expiredAt(clock.instant())) {
            log.warn("Device key rejected key_id={}", key.keyId());
            throw new UnauthenticatedException("Invalid device key");
        }
        return new DeviceIdentity(credential.get().deviceId(), key.keyId());
    }

    @Override
    public void assertBoundTo(DeviceIdentity device, long patientId, List<Instant> measuredAts) {
        if (measuredAts.isEmpty()) {
            return;
        }
        Instant from = measuredAts.stream().min(Comparator.naturalOrder()).orElseThrow();
        Instant to = measuredAts.stream().max(Comparator.naturalOrder()).orElseThrow();
        List<DeviceBinding> bindings = directory.findBindings(device.deviceId(), from, to);
        for (Instant t : measuredAts) {
            Optional<DeviceBinding> owner = bindings.stream().filter(b -> b.covers(t)).findFirst();
            if (owner.isEmpty() || owner.get().patientId() != patientId) {
                throw new DeviceMismatchException(DeviceMismatchException.Reason.DEVICE_NOT_BOUND,
                        "Device " + device.deviceId() + " was not bound to patient " + patientId + " at " + t);
            }
        }
    }
}
