package com.vitallink.readings.domain;

import java.util.List;

/** One result per request item, in request order. */
public record IngestResult(List<ItemResult> results) {
}
