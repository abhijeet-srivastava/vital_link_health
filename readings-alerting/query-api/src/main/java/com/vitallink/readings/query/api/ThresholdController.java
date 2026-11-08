package com.vitallink.readings.query.api;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.cache.CacheKeys;
import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.MeasureBounds;
import com.vitallink.readings.domain.NewBand;
import com.vitallink.readings.domain.OutboxEvent;
import com.vitallink.readings.domain.Severity;
import com.vitallink.readings.domain.ThresholdBand;
import com.vitallink.readings.domain.ThresholdChange;
import com.vitallink.readings.messaging.ThresholdsChangedV1;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.ports.MeasureCatalog;
import com.vitallink.readings.ports.OutboxWriter;
import com.vitallink.readings.ports.ThresholdRepository;
import com.vitallink.readings.query.security.AccessAudit;
import com.vitallink.readings.query.security.Caller;
import com.vitallink.readings.query.security.PanelAccessGuard;
import com.vitallink.readings.query.security.Scopes;

/**
 * Clinician replaces a patient's override bands for one measure. One SERIALIZABLE transaction closes the open rows,
 * inserts the new ones, writes the thresholds.changed outbox row (rule 2) and the audit row; a concurrent
 * replacement loses with 409 instead of leaving a merged set open. After commit the Redis entry is deleted (best
 * effort); every evaluator pod also evicts its L1 on the event, and the 10-minute TTL bounds staleness if both fail.
 */
@RestController
public class ThresholdController {

    static final String UPDATE_THRESHOLD = "UPDATE_THRESHOLD";
    static final int MAX_BANDS = 10;
    static final String SERIALIZATION_FAILURE = "40001";

    private static final Logger log = LoggerFactory.getLogger(ThresholdController.class);

    private final PanelAccessGuard guard;
    private final AccessAudit audit;
    private final ThresholdRepository thresholds;
    private final OutboxWriter outbox;
    private final MeasureCatalog measures;
    private final TransactionTemplate tx;
    private final StringRedisTemplate redis;
    private final ObjectMapper json;
    private final UuidV7 ids;
    private final Clock clock;
    private final RequestIds requestIds;

    public ThresholdController(PanelAccessGuard guard, AccessAudit audit, ThresholdRepository thresholds,
                               OutboxWriter outbox, MeasureCatalog measures, TransactionTemplate tx,
                               StringRedisTemplate redis, ObjectMapper json, UuidV7 ids, Clock clock,
                               RequestIds requestIds) {
        this.guard = guard;
        this.audit = audit;
        this.thresholds = thresholds;
        this.outbox = outbox;
        this.measures = measures;
        this.tx = new TransactionTemplate(tx.getTransactionManager());
        this.tx.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);
        this.redis = redis;
        this.json = json;
        this.ids = ids;
        this.clock = clock;
        this.requestIds = requestIds;
    }

    public record BandRequest(ThresholdBand.Operator operator, BigDecimal limit, Severity severity) {
    }

    public record ThresholdsRequest(List<BandRequest> bands) {
    }

    public record ThresholdsResponse(long patientId, Measure measure, boolean changed, List<Long> thresholdIds) {
    }

    /** Thrown inside the transaction so the outbox and audit rows roll back with the rule change. */
    static class ThresholdConflictException extends RuntimeException {
        ThresholdConflictException() {
            super("Thresholds changed concurrently; reload and retry");
        }
    }

    @PutMapping("/v1/patients/{patientId}/thresholds/{measure}")
    public ThresholdsResponse replace(@PathVariable long patientId, @PathVariable Measure measure,
                                      @RequestBody ThresholdsRequest body, @AuthenticationPrincipal Jwt jwt,
                                      @RequestHeader(value = RequestIds.HEADER, required = false)
                                      String requestHeader) {
        Caller caller = Caller.from(jwt);
        UUID requestId = requestIds.resolve(requestHeader);
        String resource = "thresholds/" + measure;
        guard.requireScope(caller, Scopes.THRESHOLDS_WRITE, patientId, UPDATE_THRESHOLD, resource, requestId);
        List<NewBand> bands = validate(measure, body);

        Optional<ThresholdChange> change;
        try {
            change = tx.execute(status -> {
                Optional<ThresholdChange> c = thresholds.replacePatientBands(patientId, measure, bands,
                        caller.auditActor() + ":" + caller.subject(), clock.instant());
                c.ifPresent(ch -> {
                    outbox.append(List.of(event(ch)));
                    audit.allowed(caller, UPDATE_THRESHOLD, patientId, resource, requestId);
                });
                return c;
            });
        } catch (DuplicateKeyException | ConcurrencyFailureException | TransactionSystemException e) {
            if (e instanceof TransactionSystemException && !serializationFailure(e)) {
                throw e;
            }
            throw new ThresholdConflictException();
        }
        change.ifPresent(c -> evict(patientId, measure));
        return new ThresholdsResponse(patientId, measure, change.isPresent(),
                change.map(ThresholdChange::insertedIds).orElse(List.of()));
    }

    /** A SERIALIZABLE conflict detected at COMMIT is not translated by the transaction manager. */
    private static boolean serializationFailure(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException sql && SERIALIZATION_FAILURE.equals(sql.getSQLState())) {
                return true;
            }
        }
        return false;
    }

    private List<NewBand> validate(Measure measure, ThresholdsRequest body) {
        if (body == null || body.bands() == null) {
            throw new InvalidRequestException("INVALID_BANDS", "bands is required (empty removes the override)");
        }
        if (body.bands().size() > MAX_BANDS) {
            throw new InvalidRequestException("INVALID_BANDS", "at most " + MAX_BANDS + " bands");
        }
        MeasureBounds bounds = measures.plausibilityRules().of(measure);
        Set<String> seen = new HashSet<>();
        return body.bands().stream().map(b -> {
            if (b == null || b.operator() == null || b.limit() == null || b.severity() == null) {
                throw new InvalidRequestException("INVALID_BANDS", "each band needs operator, limit and severity");
            }
            if (!bounds.contains(b.limit())) {
                throw new InvalidRequestException("INVALID_BANDS", "limit outside the plausible range of " + measure);
            }
            if (b.limit().scale() > 2) {
                throw new InvalidRequestException("INVALID_BANDS", "limit has more than 2 decimals");
            }
            if (!seen.add(b.operator() + "/" + b.severity())) {
                throw new InvalidRequestException("INVALID_BANDS", "one band per operator and severity");
            }
            return new NewBand(b.operator(), b.limit(), b.severity());
        }).toList();
    }

    private OutboxEvent event(ThresholdChange c) {
        UUID eventId = ids.next();
        try {
            return new OutboxEvent(eventId, ThresholdsChangedV1.TOPIC, ThresholdsChangedV1.EVENT_TYPE,
                    String.valueOf(c.patientId()), ThresholdsChangedV1.SCHEMA_VERSION,
                    json.writeValueAsString(ThresholdsChangedV1.of(eventId, c)));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("thresholds.changed payload not serializable", e);
        }
    }

    private void evict(long patientId, Measure measure) {
        try {
            redis.delete(CacheKeys.thresholds(patientId, measure));
        } catch (RuntimeException e) {
            log.warn("Threshold cache eviction in Redis failed patient_id={} measure={} error={}", patientId, measure,
                    e.getClass().getSimpleName());
        }
    }
}
