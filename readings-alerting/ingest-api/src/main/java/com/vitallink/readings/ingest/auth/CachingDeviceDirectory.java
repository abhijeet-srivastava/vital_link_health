package com.vitallink.readings.ingest.auth;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;
import com.vitallink.readings.domain.DeviceBinding;
import com.vitallink.readings.domain.DeviceCredential;
import com.vitallink.readings.ports.DeviceDirectory;

/**
 * Decorator: caches credential rows by key_id (unknown key_ids too, so probing does not reach the DB). The HMAC is
 * still checked on every request; only the row is cached. Bindings pass straight through: a rebinding must count
 * at once, because attributing a reading to the wrong patient is worse than one indexed query per request.
 * <p>
 * Async on purpose: requests run on virtual threads, and a synchronous Caffeine loader runs the DB lookup inside the
 * map's {@code synchronized} compute. On JDK 21 that pins the carrier; once every carrier waits for a pool
 * connection, the threads holding the connections can't run to return them (stage 11 load test). Here the compute
 * only installs a future, the lookup runs on {@code loader}, and the request parks in {@code join()} unpinned.
 * Expiry is jittered below the TTL (the TTL is also the revocation latency) so a warm-up doesn't expire at once.
 */
public class CachingDeviceDirectory implements DeviceDirectory {

    private final DeviceDirectory delegate;
    private final AsyncCache<String, Optional<DeviceCredential>> credentials;

    public CachingDeviceDirectory(DeviceDirectory delegate, Duration ttl, long maxSize, Executor loader) {
        this(delegate, ttl, maxSize, loader, Ticker.systemTicker());
    }

    CachingDeviceDirectory(DeviceDirectory delegate, Duration ttl, long maxSize, Executor loader, Ticker ticker) {
        this.delegate = delegate;
        long ttlNanos = ttl.toNanos();
        this.credentials = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfter(Expiry.<String, Optional<DeviceCredential>>creating((k, v) -> jittered(ttlNanos)))
                .executor(loader)
                .ticker(ticker)
                .buildAsync();
    }

    @Override
    public Optional<DeviceCredential> findActiveCredential(String keyId) {
        // Single-flight per key: concurrent misses share one future. A failed load is dropped, not cached.
        CompletableFuture<Optional<DeviceCredential>> credential = credentials.get(keyId,
                (k, executor) -> CompletableFuture.supplyAsync(() -> delegate.findActiveCredential(k), executor));
        try {
            return credential.join();
        } catch (CompletionException e) {
            credentials.asMap().remove(keyId, credential);   // Caffeine drops it too, but asynchronously
            if (e.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw new IllegalStateException("credential lookup failed", e.getCause());
        }
    }

    @Override
    public List<DeviceBinding> findBindings(long deviceId, Instant from, Instant to) {
        return delegate.findBindings(deviceId, from, to);
    }

    private static Duration jittered(long ttlNanos) {
        return Duration.ofNanos(ttlNanos - ThreadLocalRandom.current().nextLong(Math.max(1, ttlNanos / 10)));
    }
}
