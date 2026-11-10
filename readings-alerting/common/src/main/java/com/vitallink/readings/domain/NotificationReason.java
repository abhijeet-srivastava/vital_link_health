package com.vitallink.readings.domain;

/** Values match the notification.reason CHECK in references/schema.sql. */
public enum NotificationReason {
    RAISED,
    SEVERITY_UPGRADED,
    ESCALATED,
    REASSIGNED
}
