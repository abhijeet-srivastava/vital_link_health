package com.vitallink.readings.query.ai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;

import com.vitallink.readings.domain.AlertStatus;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.messaging.AlertsRaisedV1;
import com.vitallink.readings.messaging.KafkaTopics;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

/**
 * architecture.md, partial outages: with the LLM provider down, ingest and alerts are unaffected and reads fall back
 * to the rule-based summary. Ingest and evaluation run in other deployables, so this is shown two ways: their code
 * has no path to the LLM at all, and everything alert-related that query-api does keeps working while the LLM fails.
 */
@TestPropertySource(properties = "vitallink.summary.llm.stub-mode=error")
class SummaryLlmDownIsolationTest extends SummaryTestSupport {

    static final long COACH = 51;
    static final long PATIENT = 5101;

    /** Code on the ingest and evaluation paths; none of it may reach the LLM integration. */
    static final List<String> CRITICAL_PATH_MODULES = List.of("readings-alerting/ingest-api", "readings-alerting/alert-worker");
    static final List<String> LLM_MARKERS = List.of("LlmClient", "ReadingSummaryService", "query.ai", "summary:",
            "<artifactId>query-api</artifactId>");

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    KafkaListenerEndpointRegistry listeners;

    @Test
    void llm_down_ingest_and_alerts_unaffected() throws Exception {
        // 1. No path from ingest or evaluation to the LLM: no reference in their sources or build.
        for (String module : CRITICAL_PATH_MODULES) {
            Path root = repoPath(module);
            try (Stream<Path> files = Files.walk(root.resolve("src/main"))) {
                List<Path> all = Stream.concat(files.filter(Files::isRegularFile), Stream.of(root.resolve("pom.xml")))
                        .toList();
                assertThat(all).isNotEmpty();
                for (Path f : all) {
                    String text = read(f);
                    assertThat(LLM_MARKERS).as("%s references the LLM integration", f)
                            .noneMatch(text::contains);
                }
            }
        }

        // 2. The LLM fails every call: summaries degrade to rules, never a 5xx, and the circuit opens.
        primary(COACH, PATIENT);
        bp(PATIENT, now().minus(Duration.ofHours(1)), 188, 96);
        rollUp();
        for (int i = 0; i < 8; i++) {
            Response r = summary(coachToken(COACH), PATIENT);
            assertThat(r.status()).isEqualTo(200);
            assertThat(r.body().path("source").asText()).isEqualTo("rules");
        }
        assertThat(llmBreaker().getState()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(stub.calls()).as("calls stop once the circuit opens").isLessThan(8);

        // 3. Alerts keep flowing through query-api: a raised alert (as the evaluator writes it) is announced, the
        //    summary cache is evicted from the event, and the coach sees and acknowledges the alert.
        long alertId = alert(PATIENT, COACH, Measure.SYSTOLIC, Severity.CRITICAL, AlertStatus.OPEN, now());
        redisCli("SET", "summary:" + PATIENT, "{\"stale\":true}", "EX", "900");
        await().atMost(Duration.ofSeconds(30)).until(() -> !listeners
                .getListenerContainer(SummaryCacheEvictionListener.ID).getAssignedPartitions().isEmpty());
        AlertsRaisedV1 raised = new AlertsRaisedV1(UUID.randomUUID(), AlertsRaisedV1.EVENT_TYPE,
                AlertsRaisedV1.SCHEMA_VERSION, alertId, PATIENT, COACH, Measure.SYSTOLIC.name(), new BigDecimal("188"),
                Severity.CRITICAL.name(), "RAISED", 1, 1, false);
        kafka.send(new ProducerRecord<>(KafkaTopics.ALERTS_RAISED_CRITICAL, String.valueOf(PATIENT),
                JSON.writeValueAsString(raised))).get();
        await().atMost(Duration.ofSeconds(30)).until(() -> cachedSummary(PATIENT).isEmpty());

        Response queue = get(coachToken(COACH), "/v1/coaches/" + COACH + "/alerts");
        assertThat(queue.status()).isEqualTo(200);
        assertThat(texts(queue.body().path("items"), "alert_id")).containsExactly(String.valueOf(alertId));
        Response ack = send(coachToken(COACH), "POST", "/v1/alerts/" + alertId + "/acknowledge", null,
                Map.of("If-Match", "\"0\""));
        assertThat(ack.status()).isEqualTo(200);
        assertThat(alertRow(alertId)).containsEntry("status", "ACKNOWLEDGED");
        Response latest = get(coachToken(COACH), "/v1/patients/" + PATIENT + "/readings/latest?type=BLOOD_PRESSURE");
        assertThat(latest.status()).isEqualTo(200);
    }

    private static String read(Path file) {
        try {
            return Files.readString(file);
        } catch (IOException e) {
            return "";   // binary resource: cannot hold a Java reference
        }
    }
}
