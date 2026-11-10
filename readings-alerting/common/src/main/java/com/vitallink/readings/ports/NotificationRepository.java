package com.vitallink.readings.ports;

import java.time.Instant;
import java.util.List;

import com.vitallink.readings.domain.CareTeam;
import com.vitallink.readings.domain.Channel;
import com.vitallink.readings.domain.NotificationRow;
import com.vitallink.readings.domain.PendingNotification;

/** Pattern 6 in SQL. Every write is guarded by the natural key (guard 5) or by {@code status = 'PENDING'} (guard 6). */
public interface NotificationRepository {

    CareTeam currentCareTeam(long patientId);

    /** notification_policy.non_critical = DAILY_DIGEST; no row means IMMEDIATE. */
    boolean dailyDigest(long patientId);

    /**
     * ON CONFLICT (trigger_event_id, coach_id, channel) DO NOTHING; QUEUE rows are stored SENT at {@code now}.
     * Returns the rows actually inserted (a replay inserts none).
     */
    int insertAll(List<NotificationRow> rows, Instant now);

    /**
     * PENDING rows of one channel due at {@code now}, oldest first, FOR UPDATE SKIP LOCKED. Must run inside the
     * caller's transaction, which holds the row locks until the outcome is written.
     *
     * @param critical true: CRITICAL rows only; false: every other severity
     */
    List<PendingNotification> claimDue(Channel channel, boolean critical, int limit, Instant now);

    /** PENDING -> SENT; attempts + 1. */
    void markSent(long notificationId, Instant now);

    /** Stays PENDING; attempts + 1; due again at {@code nextAttemptAt}. */
    void markRetry(long notificationId, Instant nextAttemptAt);

    /** PENDING -> FAILED; attempts + 1. */
    void markFailed(long notificationId);

    /** Stays PENDING, no attempt counted (the call was not made, e.g. circuit open). */
    void reschedule(long notificationId, Instant nextAttemptAt);
}
