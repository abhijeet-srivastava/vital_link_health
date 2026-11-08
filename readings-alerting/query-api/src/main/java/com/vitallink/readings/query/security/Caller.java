package com.vitallink.readings.query.security;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.security.oauth2.jwt.Jwt;

import com.vitallink.readings.domain.AuditEntry;

/**
 * Who is calling, from a validated token: {@code sub} is the coach, clinician or service id, {@code vl_actor} its
 * kind, {@code scope} the granted scopes (space separated). The token validator has already rejected tokens without
 * a known {@code vl_actor}.
 */
public record Caller(String subject, Kind kind, Set<String> scopes) {

    public static final String ACTOR_CLAIM = "vl_actor";

    public enum Kind {
        COACH,
        CLINICIAN,
        SERVICE
    }

    public static Caller from(Jwt jwt) {
        String scope = jwt.getClaimAsString("scope");
        Set<String> scopes = scope == null ? Set.of()
                : Arrays.stream(scope.split(" ")).filter(s -> !s.isBlank()).collect(Collectors.toUnmodifiableSet());
        return new Caller(jwt.getSubject(), Kind.valueOf(jwt.getClaimAsString(ACTOR_CLAIM)), scopes);
    }

    public boolean has(String scope) {
        return scopes.contains(scope);
    }

    public boolean isCoach() {
        return kind == Kind.COACH;
    }

    /** The coach id; only meaningful for coaches, whose subject the token validator checked is numeric. */
    public long coachId() {
        if (!isCoach()) {
            throw new IllegalStateException("Not a coach: " + kind);
        }
        return Long.parseLong(subject);
    }

    /** audit_log.actor_type has no CLINICIAN: clinicians are audited as ADMIN (schema CHECK). */
    public AuditEntry.ActorType auditActor() {
        return switch (kind) {
            case COACH -> AuditEntry.ActorType.COACH;
            case CLINICIAN -> AuditEntry.ActorType.ADMIN;
            case SERVICE -> AuditEntry.ActorType.SERVICE;
        };
    }
}
