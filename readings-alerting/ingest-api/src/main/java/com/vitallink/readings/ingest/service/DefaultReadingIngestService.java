package com.vitallink.readings.ingest.service;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vitallink.readings.domain.AllItemsRejectedException;
import com.vitallink.readings.domain.AuditEntry;
import com.vitallink.readings.domain.BatchTooLargeException;
import com.vitallink.readings.domain.DeviceIdentity;
import com.vitallink.readings.domain.DeviceMismatchException;
import com.vitallink.readings.domain.ExistingHash;
import com.vitallink.readings.domain.IngestBatch;
import com.vitallink.readings.domain.IngestResult;
import com.vitallink.readings.domain.ItemResult;
import com.vitallink.readings.domain.ItemStatus;
import com.vitallink.readings.domain.MalformedBatchException;
import com.vitallink.readings.domain.OutboxEvent;
import com.vitallink.readings.domain.PlausibilityRules;
import com.vitallink.readings.domain.Reading;
import com.vitallink.readings.domain.ReadingInput;
import com.vitallink.readings.domain.ReadingKey;
import com.vitallink.readings.domain.ReadingValidator;
import com.vitallink.readings.ingest.config.IngestProperties;
import com.vitallink.readings.messaging.ReadingsIngestedV1;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.ports.AuditWriter;
import com.vitallink.readings.ports.DeviceAuthenticator;
import com.vitallink.readings.ports.LatestReadingCache;
import com.vitallink.readings.ports.MeasureCatalog;
import com.vitallink.readings.ports.OutboxWriter;
import com.vitallink.readings.ports.ReadingIngestService;
import com.vitallink.readings.ports.ReadingRepository;

/**
 * Pattern 1 (idempotency-and-concurrency.md): validate before touching the DB, then one short transaction - insert
 * ON CONFLICT DO NOTHING RETURNING, classify the rest by payload_hash, one outbox row for the new rows only, audit -
 * then a best-effort latest-reading update after commit. No alert work here (rule 4).
 */
@Service
public class DefaultReadingIngestService implements ReadingIngestService {

    private static final Logger log = LoggerFactory.getLogger(DefaultReadingIngestService.class);

    static final String AUDIT_ACTION = "INGEST_READINGS";

    private final DeviceAuthenticator authenticator;
    private final ReadingRepository readings;
    private final OutboxWriter outbox;
    private final AuditWriter audit;
    private final MeasureCatalog measures;
    private final LatestReadingCache latest;
    private final ReadingValidator validator;
    private final TransactionTemplate tx;
    private final ObjectMapper json;
    private final UuidV7 ids;
    private final Clock clock;
    private final int maxBatchSize;

    public DefaultReadingIngestService(DeviceAuthenticator authenticator, ReadingRepository readings,
                                       OutboxWriter outbox, AuditWriter audit, MeasureCatalog measures,
                                       LatestReadingCache latest, ReadingValidator validator, TransactionTemplate tx,
                                       ObjectMapper json, UuidV7 ids, Clock clock, IngestProperties props) {
        this.authenticator = authenticator;
        this.readings = readings;
        this.outbox = outbox;
        this.audit = audit;
        this.measures = measures;
        this.latest = latest;
        this.validator = validator;
        this.tx = tx;
        this.json = json;
        this.ids = ids;
        this.clock = clock;
        this.maxBatchSize = props.maxBatchSize();
    }

    @Override
    public IngestResult ingest(DeviceIdentity device, IngestBatch batch) {
        int n = batch.readings().size();
        if (n > maxBatchSize) {
            throw new BatchTooLargeException(n, maxBatchSize);
        }
        if (n == 0) {
            throw new MalformedBatchException("readings must contain at least one reading");
        }
        checkAccess(device, batch);

        ItemResult[] results = new ItemResult[n];
        List<Reading> candidates = new ArrayList<>(n);
        List<Integer> positions = new ArrayList<>(n);
        validate(device, batch, results, candidates, positions);
        if (candidates.isEmpty()) {
            throw new AllItemsRejectedException(new IngestResult(Arrays.asList(results)));
        }

        List<Reading> accepted = new ArrayList<>();
        List<Reading> conflicts = new ArrayList<>();
        tx.executeWithoutResult(status ->
                store(device, batch, candidates, positions, results, accepted, conflicts));

        for (Reading r : conflicts) {
            log.warn("Reading integrity conflict: same reading id, different values device_id={} reading_uuid={} measured_at={}",
                    r.deviceId(), r.readingUuid(), r.measuredAt());
        }
        updateLatest(batch.patientId(), accepted);
        return new IngestResult(List.of(results));
    }

    private void checkAccess(DeviceIdentity device, IngestBatch batch) {
        try {
            if (device.deviceId() != batch.deviceId()) {
                throw new DeviceMismatchException(DeviceMismatchException.Reason.KEY_FOR_OTHER_DEVICE,
                        "Key " + device.keyId() + " does not belong to device " + batch.deviceId());
            }
            List<Instant> measuredAts = batch.readings().stream()
                    .filter(Objects::nonNull)
                    .map(ReadingInput::measuredAt)
                    .filter(Objects::nonNull)
                    .toList();
            authenticator.assertBoundTo(device, batch.patientId(), measuredAts);
        } catch (DeviceMismatchException e) {
            tx.executeWithoutResult(status -> audit.record(auditEntry(device, batch, AuditEntry.Outcome.DENIED)));
            throw e;
        }
    }

    private void validate(DeviceIdentity device, IngestBatch batch, ItemResult[] results,
                          List<Reading> candidates, List<Integer> positions) {
        PlausibilityRules rules = measures.plausibilityRules();
        Instant now = clock.instant();
        for (int i = 0; i < results.length; i++) {
            ReadingInput in = batch.readings().get(i);
            if (in == null) {
                in = new ReadingInput(null, null, null, null, null, null);
            }
            switch (validator.validate(device.deviceId(), batch.patientId(), in, rules, now)) {
                case ReadingValidator.Valid v -> {
                    candidates.add(v.reading());
                    positions.add(i);
                }
                case ReadingValidator.Rejected r -> results[i] = ItemResult.rejected(r.readingId(), r.code());
            }
        }
    }

    private void store(DeviceIdentity device, IngestBatch batch, List<Reading> candidates, List<Integer> positions,
                       ItemResult[] results, List<Reading> accepted, List<Reading> conflicts) {
        Set<ReadingKey> inserted = new HashSet<>(readings.insertIgnoringDuplicates(candidates));

        List<Integer> existing = new ArrayList<>();
        for (int j = 0; j < candidates.size(); j++) {
            Reading r = candidates.get(j);
            if (inserted.remove(r.key())) {   // remove: a second copy in the same batch is a duplicate of the first
                results[positions.get(j)] = ItemResult.of(r.readingUuid(), ItemStatus.ACCEPTED);
                accepted.add(r);
            } else {
                existing.add(j);
            }
        }

        if (!existing.isEmpty()) {
            List<ReadingKey> keys = existing.stream().map(j -> candidates.get(j).key()).distinct().toList();
            Map<ReadingKey, Long> stored = new HashMap<>();
            for (ExistingHash h : readings.findPayloadHashes(device.deviceId(), keys)) {
                stored.put(h.key(), h.payloadHash());
            }
            for (int j : existing) {
                Reading r = candidates.get(j);
                Long hash = stored.get(r.key());
                if (hash == null) {
                    throw new IllegalStateException("Conflicting reading not found device_id=" + r.deviceId()
                            + " reading_uuid=" + r.readingUuid());
                }
                boolean same = hash == r.payloadHash();
                results[positions.get(j)] = ItemResult.of(r.readingUuid(), same ? ItemStatus.DUPLICATE : ItemStatus.CONFLICT);
                if (!same) {
                    conflicts.add(r);
                }
            }
        }

        if (!accepted.isEmpty()) {
            outbox.append(List.of(readingsIngested(device, batch, accepted)));
        }
        audit.record(auditEntry(device, batch, AuditEntry.Outcome.ALLOWED));
    }

    private OutboxEvent readingsIngested(DeviceIdentity device, IngestBatch batch, List<Reading> accepted) {
        var eventId = ids.next();
        ReadingsIngestedV1 payload = ReadingsIngestedV1.of(eventId, batch.patientId(), device.deviceId(), accepted);
        try {
            return new OutboxEvent(eventId, ReadingsIngestedV1.TOPIC, ReadingsIngestedV1.EVENT_TYPE,
                    String.valueOf(batch.patientId()), ReadingsIngestedV1.SCHEMA_VERSION, json.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize readings.ingested", e);
        }
    }

    private AuditEntry auditEntry(DeviceIdentity device, IngestBatch batch, AuditEntry.Outcome outcome) {
        return new AuditEntry(ids.next(), clock.instant(), AuditEntry.ActorType.DEVICE,
                String.valueOf(device.deviceId()), AUDIT_ACTION, outcome, batch.patientId(),
                "/v1/devices/" + batch.deviceId() + "/readings", batch.requestId());
    }

    /** One compare-and-set per reading type: the newest accepted reading of each type. */
    private void updateLatest(long patientId, List<Reading> accepted) {
        accepted.stream()
                .collect(Collectors.groupingBy(Reading::type,
                        Collectors.maxBy(Comparator.comparing(Reading::measuredAt))))
                .forEach((type, newest) -> newest.ifPresent(r -> latest.putIfNewer(patientId, type, r)));
    }
}
