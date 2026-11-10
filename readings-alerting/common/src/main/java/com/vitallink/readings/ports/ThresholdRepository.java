package com.vitallink.readings.ports;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.NewBand;
import com.vitallink.readings.domain.ThresholdChange;

public interface ThresholdRepository {
    /**
     * Replaces the patient's open override bands for the measure inside the caller's transaction: closes the open
     * rows (valid_to = now) and inserts the new ones. Empty when the open bands already equal {@code bands} (a
     * replayed PUT changes nothing). A concurrent replacement surfaces as a DuplicateKeyException (ux_rule_patient).
     */
    Optional<ThresholdChange> replacePatientBands(long patientId, Measure measure, List<NewBand> bands,
                                                  String createdBy, Instant now);
}
