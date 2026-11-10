package com.vitallink.readings.alerting.evaluator;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import com.vitallink.readings.messaging.KafkaTopics;

/** Open clinical decision behind vitallink.alerting.reopen-on-critical: the non-default branch. */
@SpringBootTest(properties = "vitallink.alerting.reopen-on-critical=false")
@ActiveProfiles("evaluator")
class ReopenOnCriticalFlagOffTest extends EvaluatorTestSupport {

    @Test
    void acknowledged_alert_rising_to_critical_stays_acknowledged_but_is_upgraded_and_announced() throws Exception {
        givenPatient(9001, 41, 1);
        givenSystolicBands(1);
        publish(event(9001, bp(9001, 170, Instant.now().minusSeconds(60))));
        awaitConsumed();
        jdbc.sql("""
                        UPDATE alert SET status = 'ACKNOWLEDGED', acknowledged_by = 41, acknowledged_at = now(),
                                         version = version + 1
                        WHERE patient_id = 9001
                        """)
                .update();

        publish(event(9001, bp(9001, 190)));
        awaitConsumed();

        assertThat(alerts(9001)).singleElement().satisfies(a -> {
            assertThat(a.status()).isEqualTo("ACKNOWLEDGED");
            assertThat(a.severity()).isEqualTo("CRITICAL");
            assertThat(a.escalateAt()).isNull();
            assertThat(a.occurrenceCnt()).isEqualTo(2);
        });
        assertThat(alertEvents()).extracting(OutboxRow::topic)
                .containsExactly(KafkaTopics.ALERTS_RAISED, KafkaTopics.ALERTS_RAISED_CRITICAL);
        assertThat(auditActions(9001)).containsExactly("RAISE_ALERT", "UPGRADE_ALERT");
    }
}
