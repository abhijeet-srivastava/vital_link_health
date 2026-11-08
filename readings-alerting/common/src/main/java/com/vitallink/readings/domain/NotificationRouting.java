package com.vitallink.readings.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * Changes into notifications (references/alert-processing.md): which care-team role hears about a change, on which
 * channels. QUEUE is always there (the coach queue reads the alert table); PUSH and SMS only for CRITICAL and
 * escalations. DIGEST replaces nothing and is added only for non-critical changes when the patient's policy asks for
 * it and the flag is on (open clinical decision). CRITICAL is never digested.
 */
public final class NotificationRouting {

    public enum Role {
        PRIMARY,
        BACKUP
    }

    public record Route(Role role, Channel channel) {
    }

    private NotificationRouting() {
    }

    /** @param digest the patient's notification_policy is DAILY_DIGEST and the digest flag is on */
    public static List<Route> routes(NotificationReason reason, Severity severity, boolean digest) {
        boolean critical = severity == Severity.CRITICAL;
        List<Route> routes = new ArrayList<>();
        switch (reason) {
            case RAISED, SEVERITY_UPGRADED -> {
                routes.add(new Route(Role.PRIMARY, Channel.QUEUE));
                if (critical) {
                    routes.add(new Route(Role.PRIMARY, Channel.PUSH));
                } else if (digest) {
                    routes.add(new Route(Role.PRIMARY, Channel.DIGEST));
                }
            }
            case ESCALATED -> {
                for (Role role : Role.values()) {
                    routes.add(new Route(role, Channel.QUEUE));
                    routes.add(new Route(role, Channel.PUSH));
                    routes.add(new Route(role, Channel.SMS));
                }
            }
            case REASSIGNED -> routes.add(new Route(Role.PRIMARY, Channel.QUEUE));
        }
        return routes;
    }
}
