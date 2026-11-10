package com.vitallink.readings.query.ai;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.cache.CacheKeys;
import com.vitallink.readings.domain.Prompt;
import com.vitallink.readings.domain.ReadingRollup;
import com.vitallink.readings.domain.Summary;
import com.vitallink.readings.ports.LlmClient;
import com.vitallink.readings.ports.ReadingRollupRepository;
import com.vitallink.readings.ports.ReadingSummaryService;
import com.vitallink.readings.query.security.Caller;
import com.vitallink.readings.query.security.RlsScope;

/**
 * Cache-aside summary: {@code summary:{patientId}} (Redis, 15 min, evicted when an alert is raised), else the patient's
 * hourly rollups (replica, under the coach's row-level security) -> de-identified prompt -> guarded LLM call. Any LLM
 * failure, and a patient with no recent rollups, gives the rule-based summary instead; only LLM summaries are cached,
 * so the LLM is asked again once it recovers. The read transaction ends before the LLM call, so a slow provider never
 * holds a database connection. Redis is an accelerator: its errors count as a miss or a skipped write.
 *
 * <p>Callers check the panel first (SummaryController); this class takes the coach for row-level security from the
 * request's security context.
 */
public class DefaultReadingSummaryService implements ReadingSummaryService {

    private static final Logger log = LoggerFactory.getLogger(DefaultReadingSummaryService.class);

    private final ReadingRollupRepository rollups;
    private final RlsScope rls;
    private final LlmClient llm;
    private final StringRedisTemplate redis;
    private final ObjectMapper json;
    private final Clock clock;
    private final Duration llmTimeout;
    private final Duration cacheTtl;
    private final int maxOutputChars;

    public DefaultReadingSummaryService(ReadingRollupRepository rollups, RlsScope rls, LlmClient llm,
                                        StringRedisTemplate redis, ObjectMapper json, Clock clock,
                                        Duration llmTimeout, Duration cacheTtl, int maxOutputChars) {
        this.rollups = rollups;
        this.rls = rls;
        this.llm = llm;
        this.redis = redis;
        this.json = json;
        this.clock = clock;
        this.llmTimeout = llmTimeout;
        this.cacheTtl = cacheTtl;
        this.maxOutputChars = maxOutputChars;
    }

    @Override
    public Summary summarize(long patientId, Duration window) {
        Optional<Summary> cached = cached(patientId);
        if (cached.isPresent()) {
            return cached.get();
        }
        Instant now = clock.instant();
        Caller caller = currentCaller();
        List<ReadingRollup> data = rls.read(caller, () -> rollups.findSince(patientId, now.minus(window)));
        if (data.isEmpty()) {
            return RuleBasedSummary.summarize(data, now, window);
        }
        Prompt prompt = PromptBuilder.build(data, now);
        String text;
        try {
            text = llm.complete(prompt, llmTimeout).join();
        } catch (RuntimeException e) {
            log.info("Summary from rules reason={}", reason(e));
            return RuleBasedSummary.summarize(data, now, window);
        }
        if (text == null || text.isBlank()) {
            log.info("Summary from rules reason=EMPTY_ANSWER");
            return RuleBasedSummary.summarize(data, now, window);
        }
        String trimmed = text.strip();
        Summary summary = new Summary(Summary.Source.LLM,
                trimmed.length() > maxOutputChars ? trimmed.substring(0, maxOutputChars) : trimmed, window, now);
        cache(patientId, summary);
        return summary;
    }

    private Optional<Summary> cached(long patientId) {
        try {
            String value = redis.opsForValue().get(CacheKeys.summary(patientId));
            return value == null ? Optional.empty() : Optional.of(json.readValue(value, Summary.class));
        } catch (Exception e) {
            log.warn("Summary cache read failed; building a fresh one error={}", e.getClass().getSimpleName());
            return Optional.empty();
        }
    }

    private void cache(long patientId, Summary summary) {
        try {
            redis.opsForValue().set(CacheKeys.summary(patientId), json.writeValueAsString(summary), cacheTtl);
        } catch (Exception e) {
            log.warn("Summary cache write failed error={}", e.getClass().getSimpleName());
        }
    }

    private static Caller currentCaller() {
        if (SecurityContextHolder.getContext().getAuthentication() instanceof JwtAuthenticationToken token) {
            return Caller.from(token.getToken());
        }
        throw new IllegalStateException("Summary requested outside an authenticated request");
    }

    private static String reason(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof ResilientLlmClient.LlmUnavailableException u) {
                return u.reason().name();
            }
        }
        return e.getClass().getSimpleName();
    }
}
