package com.vitallink.readings.alerting.config;

import java.time.Clock;
import java.util.Map;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.alerting.notify.AlertEventsListener;
import com.vitallink.readings.alerting.notify.ChannelSender;
import com.vitallink.readings.alerting.notify.CriticalAlertEventsListener;
import com.vitallink.readings.alerting.notify.DummySmsNotifier;
import com.vitallink.readings.alerting.notify.LoggingPushProvider;
import com.vitallink.readings.alerting.notify.NotificationRecorder;
import com.vitallink.readings.alerting.notify.ProviderRateLimiter;
import com.vitallink.readings.alerting.notify.RetryPolicy;
import com.vitallink.readings.alerting.notify.StateChangedEventsListener;
import com.vitallink.readings.domain.Channel;
import com.vitallink.readings.persistence.JdbcNotificationRepository;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Notifier: alert events -> notification rows (guard 5) -> PUSH/SMS through the providers (guard 6).
 *
 * <ul>
 *   <li>{@code notifier}: group notifier (alerts.raised) and senders for every row.</li>
 *   <li>{@code notifier-critical}: group notifier-critical (alerts.raised.critical, and alert.state_changed in a
 *       container of its own, D32) and senders for CRITICAL rows only. In production these are dedicated pods
 *       (D13); compose runs both profiles in one worker.</li>
 * </ul>
 * Both lanes in one pod share the providers' token buckets, so the CRITICAL reserve is a reserve of the same quota.
 */
@Configuration
@Profile({"notifier", "notifier-critical"})
@EnableScheduling
@EnableConfigurationProperties(NotifierProperties.class)
class NotifierConfig {

    @Bean
    JdbcNotificationRepository notificationRepository(JdbcClient jdbc) {
        return new JdbcNotificationRepository(jdbc);
    }

    @Bean
    NotificationRecorder notificationRecorder(JdbcNotificationRepository repository, TransactionTemplate tx,
                                              Clock clock, NotifierProperties props, MeterRegistry meters) {
        return new NotificationRecorder(repository, tx, clock, props.digestEnabled(), meters);
    }

    @Bean
    ChannelSender.Delivery pushDelivery(Clock clock, NotifierProperties props) {
        // Push circuit open -> an SMS row for the same change (D33); push itself is still retried.
        return ChannelSender.Delivery.of(new LoggingPushProvider(), limiter(clock, props.push()),
                props.providerTimeout()).withFallback(Channel.SMS);
    }

    @Bean
    ChannelSender.Delivery smsDelivery(Clock clock, NotifierProperties props) {
        return ChannelSender.Delivery.of(new DummySmsNotifier(), limiter(clock, props.sms()),
                props.providerTimeout());
    }

    static ChannelSender sender(ChannelSender.Lane lane, ChannelSender.Delivery push, ChannelSender.Delivery sms,
                                JdbcNotificationRepository repository, TransactionTemplate tx, Clock clock,
                                NotifierProperties props, MeterRegistry meters) {
        RetryPolicy retry = new RetryPolicy(props.maxAttempts(), props.backoffBase(), props.backoffCap(),
                props.backoffJitter());
        return new ChannelSender(lane, Map.of(Channel.PUSH, push, Channel.SMS, sms), repository, tx, clock, retry,
                props.batchSize(), meters);
    }

    private static ProviderRateLimiter limiter(Clock clock, NotifierProperties.Quota quota) {
        return new ProviderRateLimiter(clock, quota.capacity(), quota.refillPerSecond(),
                quota.criticalReservedShare());
    }

    @Configuration
    @Profile("notifier")
    static class NonCriticalLane {

        @Bean
        AlertEventsListener alertEventsListener(NotificationRecorder recorder, ObjectMapper json) {
            return new AlertEventsListener(recorder, json);
        }

        @Bean
        ChannelSender notificationSender(ChannelSender.Delivery pushDelivery, ChannelSender.Delivery smsDelivery,
                                         JdbcNotificationRepository repository, TransactionTemplate tx, Clock clock,
                                         NotifierProperties props, MeterRegistry meters) {
            return sender(ChannelSender.Lane.ALL, pushDelivery, smsDelivery, repository, tx, clock, props, meters);
        }
    }

    @Configuration
    @Profile("notifier-critical")
    static class CriticalLane {

        @Bean
        CriticalAlertEventsListener criticalAlertEventsListener(NotificationRecorder recorder, ObjectMapper json) {
            return new CriticalAlertEventsListener(recorder, json);
        }

        @Bean
        StateChangedEventsListener stateChangedEventsListener(NotificationRecorder recorder, ObjectMapper json) {
            return new StateChangedEventsListener(recorder, json);
        }

        @Bean
        ChannelSender criticalNotificationSender(ChannelSender.Delivery pushDelivery,
                                                 ChannelSender.Delivery smsDelivery,
                                                 JdbcNotificationRepository repository, TransactionTemplate tx,
                                                 Clock clock, NotifierProperties props, MeterRegistry meters) {
            return sender(ChannelSender.Lane.CRITICAL_ONLY, pushDelivery, smsDelivery, repository, tx, clock, props,
                    meters);
        }
    }
}
