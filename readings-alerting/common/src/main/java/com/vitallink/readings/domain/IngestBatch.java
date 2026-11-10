package com.vitallink.readings.domain;

import java.util.List;
import java.util.UUID;

/** One ingest request: deviceId from the path, patientId and readings from the body. */
public record IngestBatch(UUID requestId, long deviceId, long patientId, List<ReadingInput> readings) {
}
