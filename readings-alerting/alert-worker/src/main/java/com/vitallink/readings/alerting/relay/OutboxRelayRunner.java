package com.vitallink.readings.alerting.relay;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import com.vitallink.readings.messaging.OutboxRelay;

/**
 * Drives {@link OutboxRelay} on one virtual thread: a full batch is followed at once by the next, a short one by
 * {@code pollInterval}, a failed one by exponential backoff up to {@code maxBackoff}. Failed batches stay in the
 * outbox, so stopping or crashing at any point loses nothing.
 */
public class OutboxRelayRunner implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayRunner.class);

    private final OutboxRelay relay;
    private final Duration pollInterval;
    private final Duration maxBackoff;
    private final Duration stopTimeout;
    private volatile boolean running;
    private Thread worker;

    public OutboxRelayRunner(OutboxRelay relay, Duration pollInterval, Duration maxBackoff, Duration stopTimeout) {
        this.relay = relay;
        this.pollInterval = pollInterval;
        this.maxBackoff = maxBackoff;
        this.stopTimeout = stopTimeout;
    }

    @Override
    public synchronized void start() {
        running = true;
        worker = Thread.ofVirtual().name("outbox-relay").start(this::loop);
    }

    @Override
    public synchronized void stop() {
        running = false;
        worker.interrupt();
        try {
            worker.join(stopTimeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void loop() {
        Duration backoff = pollInterval;
        while (running) {
            try {
                int published = relay.publishBatch();
                backoff = pollInterval;
                if (published < OutboxRelay.BATCH_SIZE) {
                    Thread.sleep(pollInterval);
                }
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                if (!running) {
                    return;
                }
                // Messages carry topic/partition and event ids only; payloads are never logged.
                log.warn("Outbox relay batch failed; rows stay in the outbox, retrying in {}", backoff, e);
                if (!sleepQuietly(backoff)) {
                    return;
                }
                backoff = backoff.multipliedBy(2).compareTo(maxBackoff) > 0 ? maxBackoff : backoff.multipliedBy(2);
            }
        }
    }

    private static boolean sleepQuietly(Duration d) {
        try {
            Thread.sleep(d);
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }
}
