package com.vitallink.readings.ingest.api;

import java.util.List;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import com.vitallink.readings.domain.DeviceIdentity;
import com.vitallink.readings.domain.IngestBatch;
import com.vitallink.readings.domain.MalformedBatchException;
import com.vitallink.readings.domain.ReadingInput;
import com.vitallink.readings.ingest.auth.DeviceAuthInterceptor;
import com.vitallink.readings.messaging.UuidV7;
import com.vitallink.readings.ports.ReadingIngestService;

@RestController
public class ReadingsController {

    private final ReadingIngestService ingest;
    private final UuidV7 ids;

    public ReadingsController(ReadingIngestService ingest, UuidV7 ids) {
        this.ingest = ingest;
        this.ids = ids;
    }

    /** A single reading is a batch of one. 202 with one status per item, in request order. */
    @PostMapping("/v1/devices/{deviceId}/readings")
    public ResponseEntity<IngestResponse> ingest(
            @PathVariable long deviceId,
            @RequestAttribute(DeviceAuthInterceptor.DEVICE_ATTRIBUTE) DeviceIdentity device,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId,
            @RequestBody IngestRequest body) {
        if (body.patientId() == null) {
            throw new MalformedBatchException("patient_id is required");
        }
        if (body.readings() == null) {
            throw new MalformedBatchException("readings is required");
        }
        List<ReadingInput> readings = body.readings().stream()
                .map(r -> r == null ? null
                        : new ReadingInput(r.readingId(), r.deviceSeq(), r.type(), r.measuredAt(), r.values(), r.unit()))
                .toList();
        IngestBatch batch = new IngestBatch(requestId(requestId), deviceId, body.patientId(), readings);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(IngestResponse.of(ingest.ingest(device, batch)));
    }

    private UUID requestId(String header) {
        if (header != null) {
            try {
                return UUID.fromString(header);
            } catch (IllegalArgumentException ignored) {
                // not a UUID: audit_log.request_id is a UUID, so mint one
            }
        }
        return ids.next();
    }
}
