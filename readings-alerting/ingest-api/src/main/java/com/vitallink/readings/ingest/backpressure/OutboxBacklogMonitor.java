package com.vitallink.readings.ingest.backpressure;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import com.vitallink.readings.domain.OutboxBacklog;
import com.vitallink.readings.persistence.JdbcOutboxBacklog;

/**
 * Samples the outbox backlog in the background so requests only read a volatile flag. Overloaded = more rows than
 * {@code maxOutboxRows} or the oldest row older than {@code maxOutboxAge}: the relay or Kafka is not keeping up, so
 * accepting more only grows the table and delays alerts further. A failed sample keeps the last state.
 */
public class OutboxBacklogMonitor implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxBacklogMonitor.class);

    private final JdbcOutboxBacklog backlog;
    private final BackpressureProperties props;
    private volatile boolean overloaded;
    private ScheduledExecutorService sampler;

    public OutboxBacklogMonitor(JdbcOutboxBacklog backlog, BackpressureProperties props) {
        this.backlog = backlog;
        this.props = props;
    }

    public boolean overloaded() {
        return overloaded;
    }

    void sample() {
        try {
            OutboxBacklog b = backlog.sample(props.maxOutboxRows());
            boolean over = b.rows() > props.maxOutboxRows() || b.oldestAge().compareTo(props.maxOutboxAge()) > 0;
            if (over != overloaded) {
                if (over) {
                    log.warn("Outbox backlog over limit, shedding ingest rows={} oldest_age={}", b.rows(), b.oldestAge());
                } else {
                    log.info("Outbox backlog back under limit rows={} oldest_age={}", b.rows(), b.oldestAge());
                }
            }
            overloaded = over;
        } catch (RuntimeException e) {
            log.warn("Outbox backlog sample failed; keeping last state overloaded={} error={}", overloaded,
                    e.getClass().getSimpleName());
        }
    }

    @Override
    public synchronized void start() {
        sampler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().name("outbox-backlog-sampler").factory());
        long every = props.outboxSampleInterval().toMillis();
        sampler.scheduleWithFixedDelay(this::sample, 0, every, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void stop() {
        sampler.shutdownNow();
        sampler = null;
    }

    @Override
    public synchronized boolean isRunning() {
        return sampler != null;
    }
}
