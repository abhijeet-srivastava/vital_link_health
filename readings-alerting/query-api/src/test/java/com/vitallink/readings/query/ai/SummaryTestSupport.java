package com.vitallink.readings.query.ai;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;

import com.vitallink.readings.persistence.JdbcReadingRollupRepository;
import com.vitallink.readings.query.QueryApiTestSupport;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * Summary tests: real HTTP, Postgres, Redis (and Kafka where events matter). Rollups are built with the same SQL the
 * alert-worker RollupJob runs, as the owner (the job's role is not under test here). The LLM is the deterministic stub
 * behind the real ResilientLlmClient; each subclass picks the stub's mode.
 */
abstract class SummaryTestSupport extends QueryApiTestSupport {

    @Autowired
    StubLlmClient stub;

    @Autowired
    CircuitBreakerRegistry breakers;

    @AfterEach
    void closeCircuit() {
        llmBreaker().transitionToClosedState();
    }

    CircuitBreaker llmBreaker() {
        return breakers.circuitBreaker(ResilientLlmClient.BREAKER);
    }

    /** What RollupJob does on each tick, over the last day. */
    void rollUp() {
        new JdbcReadingRollupRepository(owner).refresh(Instant.now().minus(Duration.ofDays(1)));
    }

    Response summary(String token, long patientId) throws Exception {
        return get(token, "/v1/patients/" + patientId + "/summary");
    }

    String cachedSummary(long patientId) throws Exception {
        return redisCli("GET", "summary:" + patientId);
    }
}
