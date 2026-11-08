package com.vitallink.readings.ingest.auth;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.vitallink.readings.domain.DeviceBinding;
import com.vitallink.readings.domain.DeviceCredential;
import com.vitallink.readings.ports.DeviceDirectory;

/**
 * Decorator: caches credential rows by key_id (unknown key_ids too, so probing does not reach the DB). The HMAC is
 * still checked on every request; only the row is cached. Bindings pass straight through: a rebinding must count
 * at once, because attributing a reading to the wrong patient is worse than one indexed query per request.
 */
public class CachingDeviceDirectory implements DeviceDirectory {

    private final DeviceDirectory delegate;
    private final Cache<String, Optional<DeviceCredential>> credentials;

    public CachingDeviceDirectory(DeviceDirectory delegate, Duration ttl, long maxSize) {
        this.delegate = delegate;
        this.credentials = Caffeine.newBuilder().expireAfterWrite(ttl).maximumSize(maxSize).build();
    }

    @Override
    public Optional<DeviceCredential> findActiveCredential(String keyId) {
        return credentials.get(keyId, delegate::findActiveCredential);   // single-flight per key
    }

    @Override
    public List<DeviceBinding> findBindings(long deviceId, Instant from, Instant to) {
        return delegate.findBindings(deviceId, from, to);
    }
}
