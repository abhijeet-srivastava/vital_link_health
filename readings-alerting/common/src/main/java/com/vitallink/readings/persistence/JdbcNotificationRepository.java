package com.vitallink.readings.persistence;

import static com.vitallink.readings.persistence.JdbcReadingRepository.utc;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.domain.CareTeam;
import com.vitallink.readings.domain.Channel;
import com.vitallink.readings.domain.NotificationReason;
import com.vitallink.readings.domain.NotificationRow;
import com.vitallink.readings.domain.PendingNotification;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.ports.NotificationRepository;

/**
 * Pattern 6 in SQL. Recording is one unnest INSERT with ON CONFLICT on the delivery key (guard 5); sending claims
 * with SKIP LOCKED and moves PENDING -> SENT under {@code status = 'PENDING'} (guard 6). The claim filters match
 * ix_notification_due (channel, next_attempt_at) WHERE status = 'PENDING'.
 */
public class JdbcNotificationRepository implements NotificationRepository {

    private final JdbcClient jdbc;

    public JdbcNotificationRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public CareTeam currentCareTeam(long patientId) {
        record Member(long coachId, boolean primary) {
        }
        List<Member> members = jdbc.sql("""
                        SELECT coach_id, role = 'PRIMARY' FROM care_assignment
                        WHERE patient_id = ? AND valid_to IS NULL
                        ORDER BY coach_id
                        """)
                .param(patientId)
                .query((rs, n) -> new Member(rs.getLong(1), rs.getBoolean(2)))
                .list();
        Long primary = members.stream().filter(Member::primary).map(Member::coachId).findFirst().orElse(null);
        List<Long> backups = members.stream().filter(m -> !m.primary()).map(Member::coachId)
                .filter(c -> !c.equals(primary)).distinct().toList();
        return new CareTeam(primary, backups);
    }

    @Override
    public boolean dailyDigest(long patientId) {
        return jdbc.sql("SELECT non_critical = 'DAILY_DIGEST' FROM notification_policy WHERE patient_id = ?")
                .param(patientId)
                .query(Boolean.class).optional().orElse(false);
    }

    @Override
    public int insertAll(List<NotificationRow> rows, Instant now) {
        if (rows.isEmpty()) {
            return 0;
        }
        return jdbc.sql("""
                        INSERT INTO notification (trigger_event_id, alert_id, coach_id, channel, reason, severity,
                                                  status, sent_at, next_attempt_at, created_at)
                        SELECT k.trigger_event_id, k.alert_id, k.coach_id, k.channel, k.reason, k.severity,
                               CASE WHEN k.channel = 'QUEUE' THEN 'SENT' ELSE 'PENDING' END,
                               CASE WHEN k.channel = 'QUEUE' THEN ?::timestamptz END,
                               ?::timestamptz, ?::timestamptz
                        FROM unnest(?::uuid[], ?::bigint[], ?::bigint[], ?::text[], ?::text[], ?::text[])
                             AS k (trigger_event_id, alert_id, coach_id, channel, reason, severity)
                        ON CONFLICT (trigger_event_id, coach_id, channel) DO NOTHING
                        """)
                .params(utc(now), utc(now), utc(now),
                        strings(rows, r -> r.triggerEventId().toString()),
                        longs(rows, NotificationRow::alertId),
                        longs(rows, NotificationRow::coachId),
                        strings(rows, r -> r.channel().name()),
                        strings(rows, r -> r.reason().name()),
                        strings(rows, r -> r.severity().name()))
                .update();
    }

    @Override
    public List<PendingNotification> claimDue(Channel channel, boolean critical, int limit, Instant now) {
        if (limit <= 0) {
            return List.of();
        }
        return jdbc.sql("""
                        SELECT notification_id, trigger_event_id, alert_id, coach_id, channel, reason, severity,
                               attempts
                        FROM notification
                        WHERE status = 'PENDING' AND channel = ? AND next_attempt_at <= ?
                          AND (severity = 'CRITICAL') = ?
                        ORDER BY next_attempt_at, notification_id
                        LIMIT ?
                        FOR UPDATE SKIP LOCKED
                        """)
                .params(channel.name(), utc(now), critical, limit)
                .query((rs, n) -> new PendingNotification(rs.getLong(1), rs.getObject(2, UUID.class), rs.getLong(3),
                        rs.getLong(4), Channel.valueOf(rs.getString(5)), NotificationReason.valueOf(rs.getString(6)),
                        Severity.valueOf(rs.getString(7)), rs.getInt(8)))
                .list();
    }

    @Override
    public void markSent(long notificationId, Instant now) {
        jdbc.sql("""
                        UPDATE notification SET status = 'SENT', sent_at = ?, attempts = attempts + 1
                        WHERE notification_id = ? AND status = 'PENDING'
                        """)
                .params(utc(now), notificationId).update();
    }

    @Override
    public void markRetry(long notificationId, Instant nextAttemptAt) {
        jdbc.sql("""
                        UPDATE notification SET attempts = attempts + 1, next_attempt_at = ?
                        WHERE notification_id = ? AND status = 'PENDING'
                        """)
                .params(utc(nextAttemptAt), notificationId).update();
    }

    @Override
    public void markFailed(long notificationId) {
        jdbc.sql("""
                        UPDATE notification SET status = 'FAILED', attempts = attempts + 1
                        WHERE notification_id = ? AND status = 'PENDING'
                        """)
                .param(notificationId).update();
    }

    @Override
    public void reschedule(long notificationId, Instant nextAttemptAt) {
        jdbc.sql("UPDATE notification SET next_attempt_at = ? WHERE notification_id = ? AND status = 'PENDING'")
                .params(utc(nextAttemptAt), notificationId).update();
    }

    private static <T> Long[] longs(List<T> rows, Function<T, Long> f) {
        return rows.stream().map(f).toArray(Long[]::new);
    }

    private static <T> String[] strings(List<T> rows, Function<T, String> f) {
        return rows.stream().map(f).toArray(String[]::new);
    }
}
