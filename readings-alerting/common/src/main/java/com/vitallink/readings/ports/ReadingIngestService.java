package com.vitallink.readings.ports;

import com.vitallink.readings.domain.DeviceIdentity;
import com.vitallink.readings.domain.IngestBatch;
import com.vitallink.readings.domain.IngestResult;

public interface ReadingIngestService {
    /** One transaction: dedup insert + outbox for new rows. Returns per-item ACCEPTED | DUPLICATE | CONFLICT | REJECTED. */
    IngestResult ingest(DeviceIdentity device, IngestBatch batch);
}
