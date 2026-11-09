package com.vitallink.readings.ingest.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import com.vitallink.readings.domain.DeviceBinding;
import com.vitallink.readings.domain.DeviceCredential;
import com.vitallink.readings.ports.DeviceDirectory;

/**
 * The credential cache sits on every ingest request, which runs on a virtual thread. A load must never block under
 * the cache's map lock: on JDK 21 that pins the carrier, and once every carrier waits for a pool connection the
 * threads holding the connections can't run to return them (stage 11 load test, LOADTEST.md).
 */
class CachingDeviceDirectoryTest {

    static final Duration TTL = Duration.ofMinutes(5);

    final ExecutorService loader = Executors.newVirtualThreadPerTaskExecutor();

    @AfterEach
    void closeLoader() {
        loader.shutdownNow();
    }

    @Test
    void cache_misses_beyond_the_carrier_count_do_not_pin_carriers() throws Exception {
        int misses = Runtime.getRuntime().availableProcessors() + 4;
        CountDownLatch connectionReturned = new CountDownLatch(1);
        CountDownLatch waitingForConnection = new CountDownLatch(misses);
        CachingDeviceDirectory directory = new CachingDeviceDirectory(new FakeDirectory(keyId -> {
            waitingForConnection.countDown();
            await(connectionReturned);
            return Optional.of(credential(keyId));
        }), TTL, 1_000, loader);

        List<Thread> requests = new ArrayList<>();
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
                for (int i = 0; i < misses; i++) {
                    String keyId = "key-" + i;
                    requests.add(Thread.ofVirtual().start(() -> directory.findActiveCredential(keyId)));
                }
                waitingForConnection.await();
                // The connection holder is a virtual thread too: it needs a free carrier to give its connection back.
                Thread.ofVirtual().start(connectionReturned::countDown).join();
                for (Thread request : requests) {
                    request.join();
                }
            });
        } finally {
            // On failure, unblock the pinned carriers from a platform thread so later tests in this JVM still run.
            connectionReturned.countDown();
        }
    }

    @Test
    void concurrent_misses_for_one_key_load_once() throws Exception {
        AtomicInteger loads = new AtomicInteger();
        CachingDeviceDirectory directory = new CachingDeviceDirectory(new FakeDirectory(keyId -> {
            loads.incrementAndGet();
            sleep(100);
            return Optional.of(credential(keyId));
        }), TTL, 1_000, loader);

        List<Thread> requests = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            requests.add(Thread.ofVirtual().start(() -> directory.findActiveCredential("same-key")));
        }
        for (Thread request : requests) {
            request.join();
        }

        assertThat(loads).hasValue(1);
    }

    @Test
    void unknown_key_is_cached_as_empty() {
        AtomicInteger loads = new AtomicInteger();
        CachingDeviceDirectory directory = new CachingDeviceDirectory(new FakeDirectory(keyId -> {
            loads.incrementAndGet();
            return Optional.empty();
        }), TTL, 1_000, loader);

        assertThat(directory.findActiveCredential("probe")).isEmpty();
        assertThat(directory.findActiveCredential("probe")).isEmpty();
        assertThat(loads).hasValue(1);
    }

    @Test
    void failed_load_is_not_cached_and_original_exception_surfaces() {
        AtomicInteger loads = new AtomicInteger();
        CachingDeviceDirectory directory = new CachingDeviceDirectory(new FakeDirectory(keyId -> {
            if (loads.incrementAndGet() == 1) {
                throw new DataAccessResourceFailureException("pool exhausted");
            }
            return Optional.of(credential(keyId));
        }), TTL, 1_000, loader);

        assertThatThrownBy(() -> directory.findActiveCredential("k"))
                .isExactlyInstanceOf(DataAccessResourceFailureException.class);
        assertThat(directory.findActiveCredential("k")).isPresent();
        assertThat(loads).hasValue(2);
    }

    @Test
    void entries_expire_within_the_ttl_with_jitter() {
        AtomicLong nanos = new AtomicLong();
        ConcurrentHashMap<String, AtomicInteger> loads = new ConcurrentHashMap<>();
        CachingDeviceDirectory directory = new CachingDeviceDirectory(new FakeDirectory(keyId -> {
            loads.computeIfAbsent(keyId, k -> new AtomicInteger()).incrementAndGet();
            return Optional.of(credential(keyId));
        }), TTL, 1_000, Runnable::run, nanos::get);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            keys.add("key-" + i);
        }
        keys.forEach(directory::findActiveCredential);

        // Never before 0.9 x TTL: all still cached.
        nanos.set(TTL.multipliedBy(9).dividedBy(10).toNanos() - 1);
        keys.forEach(directory::findActiveCredential);
        assertThat(reloaded(keys, loads)).isZero();

        // Spread across the last 10% so a warm-up's keys don't all expire in the same second.
        nanos.set(TTL.multipliedBy(95).dividedBy(100).toNanos());
        keys.forEach(directory::findActiveCredential);
        assertThat(reloaded(keys, loads)).isBetween(1L, (long) keys.size() - 1);

        // Never after the TTL: it is also the revocation latency.
        nanos.set(TTL.toNanos());
        keys.forEach(directory::findActiveCredential);
        assertThat(reloaded(keys, loads)).isEqualTo(keys.size());
    }

    private static long reloaded(List<String> keys, ConcurrentHashMap<String, AtomicInteger> loads) {
        return keys.stream().filter(k -> loads.get(k).get() > 1).count();
    }

    private static DeviceCredential credential(String keyId) {
        return new DeviceCredential(keyId, 1, new byte[32], null);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch not released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private record FakeDirectory(Function<String, Optional<DeviceCredential>> credentials) implements DeviceDirectory {

        @Override
        public Optional<DeviceCredential> findActiveCredential(String keyId) {
            return credentials.apply(keyId);
        }

        @Override
        public List<DeviceBinding> findBindings(long deviceId, Instant from, Instant to) {
            return List.of();
        }
    }
}
