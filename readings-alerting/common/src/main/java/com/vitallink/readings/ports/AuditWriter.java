package com.vitallink.readings.ports;

import com.vitallink.readings.domain.AuditEntry;

public interface AuditWriter {
    /** INSERT only (the service role cannot update or delete audit_log). Joins the caller's transaction if any. */
    void record(AuditEntry entry);
}
