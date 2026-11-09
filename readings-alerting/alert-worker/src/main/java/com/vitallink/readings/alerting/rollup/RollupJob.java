package com.vitallink.readings.alerting.rollup;

import java.time.Clock;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.vitallink.readings.persistence.JdbcReadingRollupRepository;
import com.vitallink.readings.ports.ReadingRollupRepository;

/**
 * Keeps reading_rollup (hourly aggregates for the AI summary) current: each tick recomputes the whole hours of the last
 * {@code lookback} from reading. A reading that arrives later than the lookback (a device that buffered for hours) is
 * not rolled up; the summary is advisory and the evaluator never reads rollups. Idempotent, so safe on several
 * {@code scheduler} pods at once. Reads reading only; nothing on the ingest or evaluation path waits for it.
 */
@Component
@Profile("scheduler")
public class RollupJob {

    private static final Logger log = LoggerFactory.getLogger(RollupJob.class);

    private final ReadingRollupRepository rollups;
    private final Clock clock;
    private final Duration lookback;

    public RollupJob(JdbcClient jdbc, Clock clock, @Value("${vitallink.rollup.lookback}") Duration lookback) {
        this.rollups = new JdbcReadingRollupRepository(jdbc);
        this.clock = clock;
        this.lookback = lookback;
    }

    /** Returns the number of rollup rows written. */
    @Scheduled(initialDelayString = "PT10S", fixedDelayString = "${vitallink.rollup.interval}")
    public int run() {
        int changed = rollups.refresh(clock.instant().minus(lookback));
        log.info("Reading rollup done rows_changed={}", changed);
        return changed;
    }
}
