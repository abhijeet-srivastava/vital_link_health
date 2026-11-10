package com.vitallink.readings.alerting.notify;

import static com.vitallink.readings.domain.NotificationRouting.Role.BACKUP;
import static com.vitallink.readings.domain.NotificationRouting.Role.PRIMARY;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.vitallink.readings.domain.Channel;
import com.vitallink.readings.domain.NotificationReason;
import com.vitallink.readings.domain.NotificationRouting;
import com.vitallink.readings.domain.NotificationRouting.Route;
import com.vitallink.readings.domain.Severity;

/** The "Changes into notifications" table in references/alert-processing.md, row by row. */
class NotificationRoutingTest {

    @Test
    void new_episode_goes_to_the_primary_queue_and_push_only_if_critical() {
        assertThat(NotificationRouting.routes(NotificationReason.RAISED, Severity.HIGH, false))
                .containsExactly(new Route(PRIMARY, Channel.QUEUE));
        assertThat(NotificationRouting.routes(NotificationReason.RAISED, Severity.CRITICAL, false))
                .containsExactly(new Route(PRIMARY, Channel.QUEUE), new Route(PRIMARY, Channel.PUSH));
    }

    @Test
    void severity_upgrade_routes_like_a_new_episode() {
        assertThat(NotificationRouting.routes(NotificationReason.SEVERITY_UPGRADED, Severity.MEDIUM, false))
                .containsExactly(new Route(PRIMARY, Channel.QUEUE));
        assertThat(NotificationRouting.routes(NotificationReason.SEVERITY_UPGRADED, Severity.CRITICAL, false))
                .containsExactly(new Route(PRIMARY, Channel.QUEUE), new Route(PRIMARY, Channel.PUSH));
    }

    @Test
    void escalation_pages_primary_and_backup_on_every_channel() {
        assertThat(NotificationRouting.routes(NotificationReason.ESCALATED, Severity.CRITICAL, true))
                .containsExactlyInAnyOrder(
                        new Route(PRIMARY, Channel.QUEUE), new Route(PRIMARY, Channel.PUSH),
                        new Route(PRIMARY, Channel.SMS),
                        new Route(BACKUP, Channel.QUEUE), new Route(BACKUP, Channel.PUSH),
                        new Route(BACKUP, Channel.SMS));
    }

    @Test
    void reassignment_goes_to_the_new_coach_queue() {
        assertThat(NotificationRouting.routes(NotificationReason.REASSIGNED, Severity.CRITICAL, false))
                .containsExactly(new Route(PRIMARY, Channel.QUEUE));
    }

    @Test
    void digest_is_added_for_non_critical_only() {
        assertThat(NotificationRouting.routes(NotificationReason.RAISED, Severity.HIGH, true))
                .containsExactly(new Route(PRIMARY, Channel.QUEUE), new Route(PRIMARY, Channel.DIGEST));
        assertThat(NotificationRouting.routes(NotificationReason.RAISED, Severity.CRITICAL, true))
                .doesNotContain(new Route(PRIMARY, Channel.DIGEST));
    }
}
