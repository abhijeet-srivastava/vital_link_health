package com.vitallink.readings.ports;

import java.util.List;

import com.vitallink.readings.domain.AuditEntry;

public interface AuditWriter {
    /** INSERT only (the service role cannot update or delete audit_log). Joins the caller's transaction if any. */
    void record(AuditEntry entry);

    /** Same as {@link #record} for each entry; adapters write them in one statement. */
    default void recordAll(List<AuditEntry> entries) {
        entries.forEach(this::record);
    }
}
