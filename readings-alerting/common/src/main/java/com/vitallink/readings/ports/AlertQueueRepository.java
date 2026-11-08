package com.vitallink.readings.ports;

import java.util.Optional;

import com.vitallink.readings.domain.Alert;
import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Page;
import com.vitallink.readings.domain.QueueCursor;

public interface AlertQueueRepository {
    /**
     * A coach's unresolved alerts ordered by (severity_rank, created_at, alert_id), keyset after {@code after}.
     * {@code status} narrows to one unresolved status; RESOLVED is never returned.
     */
    Page<Alert> queue(long coachId, Optional<AlertStatus> status, QueueCursor after, int limit);
}
