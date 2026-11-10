package com.vitallink.readings.alerting.notify;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

import com.vitallink.readings.domain.AlertEvent;
import com.vitallink.readings.domain.CareTeam;
import com.vitallink.readings.domain.NotificationRouting;
import com.vitallink.readings.domain.NotificationRouting.Route;
import com.vitallink.readings.domain.NotificationRow;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.ports.NotificationRepository;
import com.vitallink.readings.ports.NotificationService;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Guard 5: one notification per (trigger event, coach, channel). Recipients come from care_assignment when the event
 * is recorded (PRIMARY, plus BACKUP for escalations); with no open PRIMARY the event's coach_id (the alert's owner)
 * is used, so a change is never dropped. A replay reuses the event id and inserts nothing.
 */
public class NotificationRecorder implements NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationRecorder.class);

    private final NotificationRepository notifications;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final boolean digestEnabled;
    private final Counter recorded;

    /** @param digestEnabled open clinical decision: honour notification_policy DAILY_DIGEST (off by default) */
    public NotificationRecorder(NotificationRepository notifications, TransactionTemplate tx, Clock clock,
                                boolean digestEnabled, MeterRegistry meters) {
        this.notifications = notifications;
        this.tx = tx;
        this.clock = clock;
        this.digestEnabled = digestEnabled;
        this.recorded = Counter.builder("vitallink.notifications.recorded")
                .description("notification rows inserted (replays insert none)").register(meters);
    }

    @Override
    public void record(AlertEvent event) {
        tx.executeWithoutResult(status -> {
            boolean digest = digestEnabled && event.severity() != Severity.CRITICAL
                    && notifications.dailyDigest(event.patientId());
            List<Route> routes = NotificationRouting.routes(event.reason(), event.severity(), digest);
            CareTeam team = notifications.currentCareTeam(event.patientId());
            List<NotificationRow> rows = rows(event, routes, team);
            int inserted = notifications.insertAll(rows, clock.instant());
            recorded.increment(inserted);
            if (team.primary() == null) {
                log.warn("No open PRIMARY; notifying the alert owner alert_id={} patient_id={} coach_id={}",
                        event.alertId(), event.patientId(), event.coachId());
            }
        });
    }

    private static List<NotificationRow> rows(AlertEvent event, List<Route> routes, CareTeam team) {
        long primary = team.primary() != null ? team.primary() : event.coachId();
        List<NotificationRow> rows = new ArrayList<>();
        for (Route route : routes) {
            List<Long> coaches = switch (route.role()) {
                case PRIMARY -> List.of(primary);
                case BACKUP -> team.backups().stream().filter(c -> c != primary).toList();
            };
            for (long coach : coaches) {
                rows.add(new NotificationRow(event.eventId(), event.alertId(), coach, route.channel(), event.reason(),
                        event.severity()));
            }
        }
        return rows;
    }
}
