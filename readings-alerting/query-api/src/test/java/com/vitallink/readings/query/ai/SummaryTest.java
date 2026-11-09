package com.vitallink.readings.query.ai;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

import com.vitallink.readings.domain.Prompt;

/** AI summary with a healthy stub LLM: circuit, prompt de-identification and panel access (testing.md: AI fallback). */
@TestPropertySource(properties = "vitallink.summary.llm.stub-mode=ok")
class SummaryTest extends SummaryTestSupport {

    static final long COACH_A = 31;
    static final long COACH_B = 32;
    static final long PATIENT = 987_654_321L;   // distinctive, so a leak into the prompt is unambiguous
    static final long PATIENT_B = 3201;

    @Test
    void open_circuit_skips_llm_and_returns_rules() throws Exception {
        primary(COACH_A, PATIENT);
        bp(PATIENT, now().minus(Duration.ofHours(2)), 150, 92);
        rollUp();
        llmBreaker().transitionToOpenState();
        int callsBefore = stub.calls();

        long started = System.nanoTime();
        Response r = summary(coachToken(COACH_A), PATIENT);
        Duration took = Duration.ofNanos(System.nanoTime() - started);

        assertThat(r.status()).isEqualTo(200);
        assertThat(r.body().path("source").asText()).isEqualTo("rules");
        assertThat(r.body().path("advisory").asBoolean()).isTrue();
        assertThat(r.body().path("text").asText()).contains("SYSTOLIC").contains("150");
        assertThat(stub.calls()).as("no LLM call while the circuit is open").isEqualTo(callsBefore);
        assertThat(took).isLessThan(Duration.ofMillis(1500));
        assertThat(cachedSummary(PATIENT)).as("a fallback is not cached").isEmpty();
    }

    @Test
    void prompt_contains_no_identifiers_or_free_text() throws Exception {
        primary(COACH_A, PATIENT);
        Instant twoHoursAgo = now().minus(Duration.ofHours(2));
        UUID first = bp(PATIENT, twoHoursAgo, 186, 94);
        UUID second = bp(PATIENT, now().minus(Duration.ofMinutes(20)), 132, 84);
        // Free text a vendor could put in extras never reaches the rollup, let alone the prompt.
        owner.sql("UPDATE reading SET extras = '{\"note\": \"Jane Doe feels dizzy\"}'::jsonb WHERE reading_uuid = ?")
                .param(first).update();
        rollUp();

        Response r = summary(coachToken(COACH_A), PATIENT);

        assertThat(r.status()).isEqualTo(200);
        assertThat(r.body().path("source").asText()).isEqualTo("llm");
        assertThat(r.body().path("advisory").asBoolean()).isTrue();
        assertThat(r.body().path("label").asText()).containsIgnoringCase("advisory");
        Prompt prompt = stub.lastPrompt();
        String sent = prompt.instructions() + "\n" + prompt.data();
        assertThat(sent).doesNotContain(String.valueOf(PATIENT), String.valueOf(COACH_A), first.toString(),
                second.toString(), "Jane", "Doe", "dizzy", String.valueOf(twoHoursAgo.atZone(java.time.ZoneOffset.UTC)
                        .getYear()));
        assertThat(sent).doesNotContainPattern("\\d{4}-\\d{2}-\\d{2}").doesNotContainPattern("\\d{1,2}:\\d{2}");
        // Every data line is a closed-form aggregate: measure, unit, hours ago, count, min, max, mean. Nothing else.
        List<String> lines = prompt.data().lines().toList();
        assertThat(lines).isNotEmpty().allMatch(l -> l.matches(
                "[A-Z0-9]+ (mmHg|bpm|mg/dL|%|kg) \\| h-\\d+ \\| n=\\d+ min=\\d+\\.\\d{2} max=\\d+\\.\\d{2} "
                        + "avg=\\d+\\.\\d{2}"), "closed-form aggregate line");
        assertThat(prompt.data()).contains("SYSTOLIC mmHg | h-2 | n=1 min=186.00");
        // The LLM summary is cached for 15 minutes under summary:{patientId}.
        assertThat(cachedSummary(PATIENT)).contains("\"source\":\"LLM\"");
        assertThat(Long.parseLong(redisCli("TTL", "summary:" + PATIENT))).isBetween(1L, 900L);
    }

    @Test
    void summary_outside_panel_is_denied_and_audited() throws Exception {
        primary(COACH_A, PATIENT);
        primary(COACH_B, PATIENT_B);
        bp(PATIENT_B, now().minus(Duration.ofHours(1)), 191, 99);
        rollUp();
        int callsBefore = stub.calls();

        Response r = send(coachToken(COACH_A), "GET", "/v1/patients/" + PATIENT_B + "/summary", null,
                java.util.Map.of("X-Request-Id", "0190f0c4-0000-7000-8000-00000000c0de"));

        assertThat(r.status()).isEqualTo(403);
        assertThat(r.contentType()).startsWith("application/problem+json");
        assertThat(r.code()).isEqualTo("ACCESS_DENIED");
        assertThat(r.body().toString()).doesNotContain("191").doesNotContain("SYSTOLIC");
        assertThat(audits("READ_SUMMARY")).singleElement().satisfies(a -> assertThat(a)
                .containsEntry("outcome", "DENIED")
                .containsEntry("actor_type", "COACH")
                .containsEntry("actor_id", String.valueOf(COACH_A))
                .containsEntry("patient_id", PATIENT_B)
                .containsEntry("resource", "summary")
                .containsEntry("request_id", UUID.fromString("0190f0c4-0000-7000-8000-00000000c0de")));
        assertThat(stub.calls()).as("no prompt built for a refused request").isEqualTo(callsBefore);
        assertThat(cachedSummary(PATIENT_B)).isEmpty();

        // The patient's own coach is served and audited ALLOWED.
        assertThat(summary(coachToken(COACH_B), PATIENT_B).status()).isEqualTo(200);
        assertThat(audits("READ_SUMMARY")).extracting(a -> a.get("outcome")).containsExactly("DENIED", "ALLOWED");
    }
}
