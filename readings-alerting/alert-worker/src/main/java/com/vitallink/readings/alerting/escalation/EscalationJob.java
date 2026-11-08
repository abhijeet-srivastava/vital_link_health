package com.vitallink.readings.alerting.escalation;

import java.time.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import com.vitallink.readings.ports.EscalationScheduler;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Escalates OPEN CRITICAL alerts whose escalate_at has passed ({@code scheduler} profile). Every pod may run it at
 * once: each batch is a SKIP LOCKED claim (guard 7). A tick drains full batches before it returns, so a backlog after
 * an outage is cleared in one tick rather than one batch per poll interval.
 */
public class EscalationJob {

    private static final Logger log = LoggerFactory.getLogger(EscalationJob.class);

    private final EscalationScheduler scheduler;
    private final Clock clock;
    private final int batchSize;
    private final Counter escalated;

    public EscalationJob(EscalationScheduler scheduler, Clock clock, int batchSize, MeterRegistry meters) {
        this.scheduler = scheduler;
        this.clock = clock;
        this.batchSize = batchSize;
        this.escalated = Counter.builder("vitallink.alerts.escalated")
                .description("OPEN CRITICAL alerts escalated after their acknowledgement window")
                .register(meters);
    }

    /** Returns the number of alerts this tick escalated. */
    @Scheduled(initialDelayString = "${vitallink.escalation.poll-interval}",
            fixedDelayString = "${vitallink.escalation.poll-interval}")
    public int run() {
        int total = 0;
        int batch;
        do {
            batch = scheduler.escalateDue(clock.instant(), batchSize);
            total += batch;
            escalated.increment(batch);
        } while (batch == batchSize);
        if (total > 0) {
            log.info("Escalated alerts count={}", total);
        }
        return total;
    }
}
