package com.vitallink.readings.query.ai;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

/** The stub hangs like a provider that never answers; the 2 s timeout must turn that into a rule-based summary. */
@TestPropertySource(properties = "vitallink.summary.llm.stub-mode=timeout")
class SummaryLlmTimeoutTest extends SummaryTestSupport {

    static final long COACH = 41;
    static final long PATIENT = 4101;

    /** LLM timeout (2 s) plus the request's own DB and Redis work. */
    static final Duration LATENCY_BUDGET = Duration.ofMillis(2800);

    @Test
    void llm_timeout_returns_rule_based_summary_within_latency_budget() throws Exception {
        primary(COACH, PATIENT);
        bp(PATIENT, now().minus(Duration.ofHours(3)), 168, 101);
        bp(PATIENT, now().minus(Duration.ofHours(1)), 142, 88);
        rollUp();

        long started = System.nanoTime();
        Response r = summary(coachToken(COACH), PATIENT);
        Duration took = Duration.ofNanos(System.nanoTime() - started);

        assertThat(r.status()).isEqualTo(200);
        assertThat(r.body().path("source").asText()).isEqualTo("rules");
        assertThat(r.body().path("advisory").asBoolean()).isTrue();
        assertThat(r.body().path("text").asText()).contains("SYSTOLIC: 2 readings, range 142-168 mmHg");
        assertThat(took).isGreaterThanOrEqualTo(Duration.ofSeconds(2)).isLessThan(LATENCY_BUDGET);
        assertThat(stub.calls()).isEqualTo(1);
        assertThat(stub.lastCall()).as("the provider's call is aborted, not left running").isCancelled();
        assertThat(cachedSummary(PATIENT)).as("a fallback is not cached").isEmpty();
        assertThat(audits("READ_SUMMARY")).singleElement()
                .satisfies(a -> assertThat(a).containsEntry("outcome", "ALLOWED").containsEntry("patient_id", PATIENT));
    }
}
