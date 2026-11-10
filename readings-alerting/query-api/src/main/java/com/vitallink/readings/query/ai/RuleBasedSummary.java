package com.vitallink.readings.query.ai;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.ReadingRollup;
import com.vitallink.readings.domain.Summary;

/**
 * The fallback when the LLM is unavailable (timeout, open circuit, rate limit, budget) and the answer for a patient with
 * no recent readings: per measure, count, range, mean and the most recent hour, computed from the same rollups the
 * prompt uses. Deterministic and local, so it always answers.
 */
public final class RuleBasedSummary {

    private RuleBasedSummary() {
    }

    public static Summary summarize(List<ReadingRollup> rollups, Instant now, Duration window) {
        return new Summary(Summary.Source.RULES, text(rollups, now, window), window, now);
    }

    static String text(List<ReadingRollup> rollups, Instant now, Duration window) {
        if (rollups.isEmpty()) {
            return "No readings in the last " + window.toHours() + " hours.";
        }
        Map<Measure, List<ReadingRollup>> byMeasure = new TreeMap<>();
        rollups.forEach(r -> byMeasure.computeIfAbsent(r.measure(), m -> new ArrayList<>()).add(r));
        List<String> sentences = new ArrayList<>();
        byMeasure.forEach((measure, hours) -> {
            int count = hours.stream().mapToInt(ReadingRollup::count).sum();
            BigDecimal min = hours.stream().map(ReadingRollup::min).min(Comparator.naturalOrder()).orElseThrow();
            BigDecimal max = hours.stream().map(ReadingRollup::max).max(Comparator.naturalOrder()).orElseThrow();
            BigDecimal weighted = hours.stream()
                    .map(h -> h.avg().multiply(BigDecimal.valueOf(h.count())))
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            BigDecimal mean = weighted.divide(BigDecimal.valueOf(count), 0, RoundingMode.HALF_UP);
            Instant latest = hours.stream().map(ReadingRollup::hourStart).max(Comparator.naturalOrder()).orElseThrow();
            long hoursAgo = Math.max(0, Duration.between(latest, now).toHours());
            sentences.add("%s: %d reading%s, range %s-%s %s, mean %s, latest %s.".formatted(measure.name(), count,
                    count == 1 ? "" : "s", plain(min), plain(max), PromptBuilder.unit(measure), plain(mean),
                    hoursAgo == 0 ? "this hour" : hoursAgo + " h ago"));
        });
        return "Last " + window.toHours() + " hours. " + String.join(" ", sentences);
    }

    private static String plain(BigDecimal v) {
        return v.stripTrailingZeros().toPlainString();
    }
}
