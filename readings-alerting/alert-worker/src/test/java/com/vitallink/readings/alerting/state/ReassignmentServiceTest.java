package com.vitallink.readings.alerting.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.vitallink.readings.domain.AlertConflict;
import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Severity;

/** Panel change: unresolved alerts follow the new PRIMARY coach; each move is audited and announced (REASSIGNED). */
class ReassignmentServiceTest extends AlertStateTestSupport {

    @Test
    void reassignment_moves_unresolved_alerts_and_emits_reassigned() {
        seedPrimary(PATIENT, COACH);
        long open = openCritical(PATIENT);
        long acknowledged = insertAlert(PATIENT, Measure.PULSE, Severity.HIGH, AlertStatus.ACKNOWLEDGED, null);
        long resolved = insertAlert(PATIENT, Measure.SPO2, Severity.MEDIUM, AlertStatus.RESOLVED, null);

        assertThat(reassignment().reassign(PATIENT, OTHER_COACH, UUID.randomUUID())).isEqualTo(2);

        assertThat(alertRow(open)).containsEntry("coach_id", OTHER_COACH).containsEntry("version", 1)
                .containsEntry("status", "OPEN");
        assertThat(alertRow(open).get("escalate_at")).isNotNull();   // the timer keeps running for the new owner
        assertThat(alertRow(acknowledged)).containsEntry("coach_id", OTHER_COACH).containsEntry("version", 1)
                .containsEntry("status", "ACKNOWLEDGED");
        assertThat(alertRow(resolved)).containsEntry("coach_id", COACH).containsEntry("version", 0);

        List<JsonNode> events = stateChangedEvents();
        assertThat(events).hasSize(2).allSatisfy(e -> {
            assertThat(e.get("reason").asText()).isEqualTo("REASSIGNED");
            assertThat(e.get("coach_id").asLong()).isEqualTo(OTHER_COACH);
            assertThat(e.get("previous_coach_id").asLong()).isEqualTo(COACH);
            assertThat(e.get("from").asText()).isEqualTo(e.get("to").asText());
        });
        assertThat(audits("REASSIGN_ALERT")).hasSize(2);

        List<Map<String, Object>> primaries = owner.sql("""
                        SELECT coach_id, valid_to IS NULL AS current FROM care_assignment
                        WHERE patient_id = ? AND role = 'PRIMARY' ORDER BY valid_from
                        """)
                .param(PATIENT).query().listOfRows();
        assertThat(primaries).extracting(r -> r.get("coach_id") + ":" + r.get("current"))
                .containsExactly(COACH + ":false", OTHER_COACH + ":true");

        // Replay of the same panel change: nothing moves, nothing is announced.
        clock.advance(WINDOW);
        assertThat(reassignment().reassign(PATIENT, OTHER_COACH, UUID.randomUUID())).isZero();
        assertThat(stateChangedEvents()).hasSize(2);
        assertThat(owner.sql("SELECT count(*) FROM care_assignment WHERE patient_id = ?").param(PATIENT)
                .query(Integer.class).single()).isEqualTo(2);
    }

    @Test
    void stale_click_after_reassignment_conflicts() {
        seedPrimary(PATIENT, COACH);
        long alertId = openCritical(PATIENT);
        reassignment().reassign(PATIENT, OTHER_COACH, UUID.randomUUID());

        assertThatThrownBy(() -> alertService().acknowledge(alertId, COACH, 0, UUID.randomUUID()))
                .isInstanceOf(AlertConflict.class);
    }
}
