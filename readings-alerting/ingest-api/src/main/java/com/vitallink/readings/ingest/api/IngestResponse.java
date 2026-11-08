package com.vitallink.readings.ingest.api;

import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.vitallink.readings.domain.IngestResult;

public record IngestResponse(List<Item> results) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Item(UUID readingId, String status, String code) {
    }

    public static IngestResponse of(IngestResult result) {
        return new IngestResponse(result.results().stream()
                .map(r -> new Item(r.readingId(), r.status().name(), r.code() == null ? null : r.code().name()))
                .toList());
    }
}
