package com.vitallink.readings.query.ai;

import java.time.Clock;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.persistence.JdbcReadingRollupRepository;
import com.vitallink.readings.ports.LlmClient;
import com.vitallink.readings.ports.ReadingSummaryService;
import com.vitallink.readings.query.security.RlsScope;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;

/** AI summary wiring: the stub LLM behind ResilientLlmClient, rollups from the replica, Redis cache. */
@Configuration
@EnableConfigurationProperties(SummaryConfig.SummaryProperties.class)
class SummaryConfig {

    /**
     * {@code vitallink.summary.*}; defaults live in application.yml.
     *
     * @param window   readings the summary covers (back from now)
     * @param cacheTtl lifetime of summary:{patientId} (an LLM summary only)
     * @param llm      LLM guards
     */
    @ConfigurationProperties("vitallink.summary")
    record SummaryProperties(Duration window, Duration cacheTtl, Llm llm) {

        /**
         * @param stubMode         {@code ok}, {@code timeout} or {@code error} (VITALLINK_LLM_STUB_MODE); no real provider
         * @param timeout          hard limit per call; the request then answers from rules
         * @param ratePerSecond    calls per second per pod; above it, rules
         * @param dailyTokenBudget tokens per pod per UTC day; above it, rules until midnight
         * @param maxOutputChars   longer answers are cut
         * @param failureRate      % of failed calls (of the last 20, at least 5) that opens the circuit
         * @param openFor          how long the circuit stays open before a trial call
         */
        record Llm(StubLlmClient.Mode stubMode, Duration timeout, int ratePerSecond, long dailyTokenBudget,
                   int maxOutputChars, float failureRate, Duration openFor) {
        }
    }

    @Bean
    StubLlmClient stubLlmClient(SummaryProperties props) {
        return new StubLlmClient(props.llm().stubMode());
    }

    @Bean
    @Primary
    LlmClient llmClient(StubLlmClient stub, CircuitBreakerRegistry breakers, SummaryProperties props, Clock clock) {
        SummaryProperties.Llm llm = props.llm();
        CircuitBreakerConfig breaker = CircuitBreakerConfig.custom()
                .slidingWindowSize(20)
                .minimumNumberOfCalls(5)
                .failureRateThreshold(llm.failureRate())
                .waitDurationInOpenState(llm.openFor())
                .build();
        RateLimiter limiter = RateLimiter.of("llm", RateLimiterConfig.custom()
                .limitForPeriod(llm.ratePerSecond())
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(Duration.ZERO)   // never queue a request behind the limit: answer from rules
                .build());
        return new ResilientLlmClient(stub, breakers.circuitBreaker(ResilientLlmClient.BREAKER, breaker), limiter,
                llm.dailyTokenBudget(), llm.timeout(), clock);
    }

    @Bean
    ReadingSummaryService readingSummaryService(@Qualifier("replicaJdbcClient") JdbcClient replica, RlsScope rls,
                                                LlmClient llm, StringRedisTemplate redis, ObjectMapper json,
                                                Clock clock, SummaryProperties props) {
        return new DefaultReadingSummaryService(new JdbcReadingRollupRepository(replica), rls, llm, redis, json, clock,
                props.llm().timeout(), props.cacheTtl(), props.llm().maxOutputChars());
    }
}
