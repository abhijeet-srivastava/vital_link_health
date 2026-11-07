// Template: port interfaces for the Readings & Alerting service (one file for review; split into one
// public interface per file when copying into the codebase).
// Domain code depends on these; adapters in persistence/ and messaging/ implement them.
// Keep signatures idempotent by design: every write takes a natural key or returns what it actually changed.

package com.vitallink.readings.ports;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

// ---------- Write path ----------

public interface DeviceAuthenticator {
    /** Resolves "vl_<keyId>_<secret>"; constant-time HMAC compare; cached. Throws UnauthenticatedException -> 401. */
    DeviceIdentity authenticate(String apiKey);

    /** Patient the device was bound to at {@code measuredAt}; throws DeviceMismatchException -> 403 if not {@code patientId}. */
    void assertBoundTo(DeviceIdentity device, long patientId, Instant measuredAt);
}

public interface ReadingIngestService {
    /** One transaction: dedup insert + outbox for new rows. Returns per-item ACCEPTED | DUPLICATE | CONFLICT | REJECTED. */
    IngestResult ingest(DeviceIdentity device, IngestBatch batch);
}

public interface ReadingRepository {
    /** INSERT ... ON CONFLICT DO NOTHING RETURNING reading_uuid. Rows not returned already existed. */
    List<UUID> insertIgnoringDuplicates(List<Reading> batch);

    /** payload_hash of existing rows, to classify DUPLICATE vs CONFLICT. */
    List<ExistingHash> findPayloadHashes(long deviceId, List<ReadingKey> keys);

    /** Keyset pagination on (measured_at DESC, reading_uuid). Never OFFSET. */
    Page<Reading> findRange(long patientId, ReadingType type, Instant from, Instant to, Cursor cursor, int limit);
}

public interface OutboxWriter {
    /** Must be called inside the caller's transaction. event_id = UUIDv7. */
    void append(List<OutboxEvent> events);
}

public interface LatestReadingCache {
    /** Lua compare-and-set on measured_at; best effort, never throws to the caller. */
    void putIfNewer(long patientId, ReadingType type, Reading reading);

    Optional<Reading> get(long patientId, ReadingType type);
}

// ---------- Alert path ----------

public interface MeasureEvaluator {                       // Strategy: one per reading type
    ReadingType supports();

    /** Most severe band crossed per measure; empty when no band is crossed. */
    List<Breach> evaluate(Reading reading, EffectiveThresholds thresholds);
}

public interface ThresholdProvider {                      // CachingThresholdProvider decorates DbThresholdProvider
    /** Patient overrides replace program defaults per measure; several programs -> most conservative. */
    EffectiveThresholds forPatients(List<Long> patientIds);
}

public interface AlertService {
    /** Set-based, one transaction for the whole batch; throws DuplicateKeyException to trigger the per-event fallback. */
    BatchOutcome raiseBatch(List<BreachWithReading> breaches);

    /** Per-event path; a duplicate occurrence means "already evaluated" and returns empty. */
    Optional<AlertChange> raise(BreachWithReading breach);

    Alert acknowledge(long alertId, long coachId, int expectedVersion);           // 409 on version mismatch

    Alert resolve(long alertId, long coachId, int expectedVersion, String note);  // note required for CRITICAL
}

public interface EscalationScheduler {
    /** Claims due alerts with SKIP LOCKED; escalates each once. Returns the number escalated. */
    int escalateDue(Instant now, int limit);
}

public interface NotificationService {
    /** INSERT ... ON CONFLICT (trigger_event_id, coach_id, channel) DO NOTHING per recipient and channel. */
    void record(AlertEvent event);
}

public interface NotificationSender {
    /** Claims due PENDING rows for one channel with SKIP LOCKED; provider idempotency key = notification_id. */
    int sendDue(Channel channel, int limit);
}

// ---------- AI (never on the ingest or evaluation path) ----------

public interface LlmClient {
    CompletableFuture<String> complete(Prompt prompt, Duration timeout);
}

public interface ReadingSummaryService {
    /** LLM summary from de-identified rollups; falls back to a rule-based summary on timeout or open circuit. */
    Summary summarize(long patientId, Duration window);
}
