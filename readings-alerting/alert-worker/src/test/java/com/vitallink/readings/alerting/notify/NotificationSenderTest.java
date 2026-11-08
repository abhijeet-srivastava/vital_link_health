package com.vitallink.readings.alerting.notify;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import com.vitallink.readings.alerting.state.MutableClock;
import com.vitallink.readings.domain.Channel;
import com.vitallink.readings.domain.DeliveryException;
import com.vitallink.readings.domain.PendingNotification;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.persistence.JdbcNotificationRepository;
import com.vitallink.readings.ports.DeliveryProvider;
import com.vitallink.readings.testsupport.IntegrationTestBase;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Guard 6 / pattern 6 against real PostgreSQL: SKIP LOCKED claim, PENDING -> SENT, provider idempotency key =
 * notification_id, durable backoff through next_attempt_at. Senders run as {@code vitallink_service}. Time is a
 * {@link MutableClock}: backoff is crossed by moving the clock, never by sleeping.
 */
class NotificationSenderTest extends IntegrationTestBase {

    static final Instant T0 = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    static final RetryPolicy RETRY = new RetryPolicy(4, Duration.ofSeconds(10), Duration.ofMinutes(1), 0.0);

    final MutableClock clock = new MutableClock(T0);
    final JdbcClient owner = JdbcClient.create(dataSource());
    final DataSource service = asServiceRole(dataSource());
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    @Test
    void sender_marks_sent_once_with_two_senders_racing() throws Exception {
        List<Long> ids = givenPending(200, Channel.PUSH, Severity.HIGH);
        LoggingPushProvider provider = new LoggingPushProvider();
        ProviderRateLimiter limiter = new ProviderRateLimiter(clock, 10_000, 0, 0.2);

        CountDownLatch start = new CountDownLatch(1);
        List<Future<Integer>> runs = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            for (int i = 0; i < 2; i++) {
                ChannelSender sender = sender(ChannelSender.Lane.ALL, Channel.PUSH, provider, limiter, 7);
                runs.add(pool.submit(() -> {
                    start.await();
                    return sender.run();
                }));
            }
            start.countDown();
        }

        int sent = 0;
        for (Future<Integer> run : runs) {
            sent += run.get();
        }
        assertThat(sent).isEqualTo(ids.size());
        assertThat(rows()).allSatisfy(r -> {
            assertThat(r.get("status")).isEqualTo("SENT");
            assertThat(r.get("attempts")).isEqualTo(1);
            assertThat(r.get("sent_at")).isNotNull();
        });
        Map<Long, Integer> calls = provider.calls();
        assertThat(calls.keySet()).containsExactlyInAnyOrderElementsOf(ids);
        assertThat(calls.values()).as("each notification_id reached the provider once").containsOnly(1);
        assertThat(meters.counter("vitallink.notifications.sent", "channel", "PUSH").count()).isEqualTo(200);
    }

    @Test
    void provider_failure_backs_off_then_fails_after_max_attempts() {
        long id = givenPending(1, Channel.SMS, Severity.CRITICAL).get(0);
        AtomicInteger calls = new AtomicInteger();
        DeliveryProvider failing = new DeliveryProvider() {
            @Override
            public String name() {
                return "sms";
            }

            @Override
            public void deliver(PendingNotification n) {
                calls.incrementAndGet();
                throw new DeliveryException("provider unavailable notification_id=" + n.notificationId());
            }
        };
        ChannelSender sender = sender(ChannelSender.Lane.ALL, Channel.SMS, failing,
                new ProviderRateLimiter(clock, 100, 100, 0.2), 20);

        Duration[] expectedBackoff = {Duration.ofSeconds(10), Duration.ofSeconds(20), Duration.ofSeconds(40)};
        for (int attempt = 1; attempt <= 3; attempt++) {
            assertThat(sender.run()).isEqualTo(1);
            Map<String, Object> row = rows().get(0);
            assertThat(row.get("status")).isEqualTo("PENDING");
            assertThat(row.get("attempts")).isEqualTo(attempt);
            assertThat(instant(row.get("next_attempt_at"))).isEqualTo(clock.instant().plus(expectedBackoff[attempt - 1]));

            clock.advance(expectedBackoff[attempt - 1].minusSeconds(1));
            assertThat(sender.run()).as("not due yet").isZero();
            clock.advance(Duration.ofSeconds(1));
        }

        assertThat(sender.run()).isEqualTo(1);   // fourth attempt = max attempts
        assertThat(rows().get(0)).containsEntry("status", "FAILED").containsEntry("attempts", 4)
                .containsEntry("notification_id", id);
        clock.advance(Duration.ofHours(1));
        assertThat(sender.run()).as("FAILED is terminal").isZero();
        assertThat(calls.get()).isEqualTo(4);
        assertThat(meters.counter("vitallink.notifications.failed", "channel", "SMS").count()).isEqualTo(1);
    }

    @Test
    void reserved_share_keeps_tokens_for_critical_when_non_critical_drains_the_bucket() {
        givenPending(20, Channel.PUSH, Severity.HIGH);
        givenPending(2, Channel.PUSH, Severity.CRITICAL);
        LoggingPushProvider provider = new LoggingPushProvider();
        // 10 tokens, no refill, 20% reserved: non-critical may take 8 at most, CRITICAL may take all 10.
        ProviderRateLimiter limiter = new ProviderRateLimiter(clock, 10, 0, 0.2);
        ChannelSender sender = sender(ChannelSender.Lane.ALL, Channel.PUSH, provider, limiter, 50);

        assertThat(sender.run()).isEqualTo(2 + 6);   // CRITICAL first (10 -> 8), then HIGH down to the 2 reserved
        assertThat(sentBySeverity()).containsEntry("CRITICAL", 2).containsEntry("HIGH", 6);

        givenPending(1, Channel.PUSH, Severity.CRITICAL);
        assertThat(sender.run()).isEqualTo(1);       // the reserve is still there for a CRITICAL
        assertThat(sentBySeverity()).containsEntry("CRITICAL", 3).containsEntry("HIGH", 6);
    }

    @Test
    void critical_only_lane_leaves_non_critical_rows_alone() {
        givenPending(3, Channel.PUSH, Severity.HIGH);
        givenPending(1, Channel.PUSH, Severity.CRITICAL);
        ChannelSender sender = sender(ChannelSender.Lane.CRITICAL_ONLY, Channel.PUSH, new LoggingPushProvider(),
                new ProviderRateLimiter(clock, 100, 100, 0.2), 20);

        assertThat(sender.run()).isEqualTo(1);
        assertThat(sentBySeverity()).containsOnlyKeys("CRITICAL");
    }

    @Test
    void push_circuit_open_falls_back_to_sms_once_and_push_still_goes_out_when_it_closes() {
        long pushId = givenPending(1, Channel.PUSH, Severity.CRITICAL).get(0);
        LoggingPushProvider push = new LoggingPushProvider();
        DummySmsNotifier sms = new DummySmsNotifier();
        ChannelSender.Delivery pushDelivery = ChannelSender.Delivery.of(push,
                new ProviderRateLimiter(clock, 100, 100, 0.2), Duration.ofSeconds(2)).withFallback(Channel.SMS);
        pushDelivery.breaker().transitionToForcedOpenState();
        ChannelSender sender = new ChannelSender(ChannelSender.Lane.ALL, Map.of(
                Channel.PUSH, pushDelivery,
                Channel.SMS, ChannelSender.Delivery.of(sms, new ProviderRateLimiter(clock, 100, 100, 0.3),
                        Duration.ofSeconds(2))),
                new JdbcNotificationRepository(JdbcClient.create(service)),
                new TransactionTemplate(new DataSourceTransactionManager(service)), clock, RETRY, 20, meters);

        assertThat(sender.run()).as("PUSH not called, then the fallback SMS sent in the same tick").isEqualTo(2);
        Map<String, Object> smsRow = row(Channel.SMS);
        assertThat(smsRow).containsEntry("status", "SENT").containsEntry("coach_id", 41L)
                .containsEntry("reason", "ESCALATED").containsEntry("severity", "CRITICAL");
        assertThat(smsRow.get("trigger_event_id")).isEqualTo(row(Channel.PUSH).get("trigger_event_id"));
        assertThat(row(Channel.PUSH)).containsEntry("status", "PENDING").containsEntry("attempts", 0)
                .containsEntry("notification_id", pushId);
        assertThat(instant(row(Channel.PUSH).get("next_attempt_at"))).isEqualTo(T0.plusSeconds(30));
        assertThat(push.calls()).isEmpty();

        clock.advance(Duration.ofSeconds(30));
        assertThat(sender.run()).as("still open: the fallback is not recorded twice").isEqualTo(1);
        assertThat(count(Channel.SMS)).isEqualTo(1);

        pushDelivery.breaker().transitionToClosedState();
        clock.advance(Duration.ofSeconds(30));
        assertThat(sender.run()).isEqualTo(1);
        assertThat(row(Channel.PUSH)).containsEntry("status", "SENT").containsEntry("attempts", 1);
        assertThat(sms.calls()).hasSize(1);
        assertThat(push.calls()).containsOnlyKeys(pushId);
        assertThat(meters.counter("vitallink.notifications.fallback", "from", "PUSH", "to", "SMS").count())
                .isEqualTo(1);
    }

    @Test
    void push_fallback_adds_no_sms_when_the_change_already_pages_by_sms() {
        // An escalation: PUSH and SMS rows for the same event and coach.
        owner.sql("""
                        WITH a AS (INSERT INTO alert (patient_id, measure_code, coach_id, severity, status,
                                                      first_seen_at, last_seen_at)
                                   VALUES (300000, 1, 41, 'CRITICAL', 'ESCALATED', now(), now()) RETURNING alert_id)
                        INSERT INTO notification (trigger_event_id, alert_id, coach_id, channel, reason, severity,
                                                  next_attempt_at)
                        SELECT 'a0000000-0000-7000-8000-000000000001'::uuid, alert_id, 41, c, 'ESCALATED', 'CRITICAL', ?
                        FROM a, unnest(ARRAY['PUSH', 'SMS']) c
                        """)
                .param(T0.atOffset(ZoneOffset.UTC)).update();
        ChannelSender.Delivery pushDelivery = ChannelSender.Delivery.of(new LoggingPushProvider(),
                new ProviderRateLimiter(clock, 100, 100, 0.2), Duration.ofSeconds(2)).withFallback(Channel.SMS);
        pushDelivery.breaker().transitionToForcedOpenState();
        DummySmsNotifier sms = new DummySmsNotifier();
        ChannelSender sender = new ChannelSender(ChannelSender.Lane.ALL, Map.of(
                Channel.PUSH, pushDelivery,
                Channel.SMS, ChannelSender.Delivery.of(sms, new ProviderRateLimiter(clock, 100, 100, 0.3),
                        Duration.ofSeconds(2))),
                new JdbcNotificationRepository(JdbcClient.create(service)),
                new TransactionTemplate(new DataSourceTransactionManager(service)), clock, RETRY, 20, meters);

        sender.run();

        assertThat(count(Channel.SMS)).isEqualTo(1);
        assertThat(sms.calls()).hasSize(1);
        assertThat(row(Channel.PUSH)).containsEntry("status", "PENDING");
    }

    // ----- support -----

    Map<String, Object> row(Channel channel) {
        return owner.sql("""
                        SELECT notification_id, trigger_event_id, coach_id, reason, severity, status, attempts,
                               next_attempt_at
                        FROM notification WHERE channel = ?
                        """)
                .param(channel.name()).query().singleRow();
    }

    int count(Channel channel) {
        return owner.sql("SELECT count(*) FROM notification WHERE channel = ?").param(channel.name())
                .query(Integer.class).single();
    }

    ChannelSender sender(ChannelSender.Lane lane, Channel channel, DeliveryProvider provider,
                         ProviderRateLimiter limiter, int batchSize) {
        return new ChannelSender(lane, Map.of(channel, ChannelSender.Delivery.of(provider, limiter,
                        Duration.ofSeconds(2))),
                new JdbcNotificationRepository(JdbcClient.create(service)),
                new TransactionTemplate(new DataSourceTransactionManager(service)), clock, RETRY, batchSize, meters);
    }

    /** PENDING rows, each for its own alert, due at T0. */
    List<Long> givenPending(int n, Channel channel, Severity severity) {
        return owner.sql("""
                        WITH a AS (
                          INSERT INTO alert (patient_id, measure_code, coach_id, severity, status, first_seen_at,
                                             last_seen_at)
                          SELECT 200000 + coalesce((SELECT max(patient_id) - 200000 + 1 FROM alert
                                                     WHERE patient_id >= 200000), 0) + g,
                                 1, 41, ?, 'ESCALATED', now(), now()
                          FROM generate_series(0, ? - 1) g
                          RETURNING alert_id, severity)
                        INSERT INTO notification (trigger_event_id, alert_id, coach_id, channel, reason, severity,
                                                  next_attempt_at)
                        SELECT gen_random_uuid(), alert_id, 41, ?, 'ESCALATED', severity, ? FROM a
                        RETURNING notification_id
                        """)
                .params(severity.name(), n, channel.name(), T0.atOffset(ZoneOffset.UTC))
                .query(Long.class).list();
    }

    List<Map<String, Object>> rows() {
        return owner.sql("""
                        SELECT notification_id, status, attempts, next_attempt_at, sent_at FROM notification
                        ORDER BY notification_id
                        """)
                .query().listOfRows();
    }

    Map<String, Integer> sentBySeverity() {
        Map<String, Integer> result = new HashMap<>();
        owner.sql("SELECT severity, count(*)::int FROM notification WHERE status = 'SENT' GROUP BY severity")
                .query((rs, n) -> result.put(rs.getString(1), rs.getInt(2))).list();
        return result;
    }

    static Instant instant(Object timestamp) {
        return timestamp instanceof OffsetDateTime o ? o.toInstant() : ((Timestamp) timestamp).toInstant();
    }

    static DataSource asServiceRole(DataSource ownerDs) {
        return new DelegatingDataSource(ownerDs) {
            @Override
            public Connection getConnection() throws SQLException {
                Connection c = super.getConnection();
                try (Statement st = c.createStatement()) {
                    st.execute("SET ROLE vitallink_service");
                }
                return c;
            }
        };
    }
}
