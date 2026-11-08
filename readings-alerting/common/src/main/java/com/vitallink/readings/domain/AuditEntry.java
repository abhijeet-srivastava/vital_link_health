package com.vitallink.readings.domain;

import java.time.Instant;
import java.util.UUID;

/** An audit_log row. resource names what was touched, never values. */
public record AuditEntry(UUID auditId, Instant at, ActorType actorType, String actorId, String action,
                         Outcome outcome, Long patientId, String resource, UUID requestId) {

    public enum ActorType { DEVICE, COACH, SERVICE, ADMIN }

    public enum Outcome { ALLOWED, DENIED }
}
