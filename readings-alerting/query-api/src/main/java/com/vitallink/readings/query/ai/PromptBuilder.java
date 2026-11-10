package com.vitallink.readings.query.ai;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import com.vitallink.readings.domain.Measure;
import com.vitallink.readings.domain.Prompt;
import com.vitallink.readings.domain.ReadingRollup;

/**
 * De-identified prompt from hourly rollups (security-phi.md, AI features). The input type carries no patient, device or
 * reading id and no free text; this class adds none: times become whole hours ago, and every data line is
 * {@code MEASURE unit | h-N | n=.. min=.. max=.. avg=..}. Nothing from a request or a person goes into the text.
 */
public final class PromptBuilder {

    static final String INSTRUCTIONS = """
            You summarise home vital-sign measurements for a health coach. Each data line is one hour of one measure: \
            measure and unit, hours before now (h-0 is the current hour), count, minimum, maximum and mean. \
            Write at most four short plain-language sentences about levels and trends. Use only the numbers given. \
            Do not diagnose, do not suggest medication or treatment, and do not judge urgency: alert severity is set \
            by clinical threshold rules, not by you.""";

    private PromptBuilder() {
    }

    public static Prompt build(List<ReadingRollup> rollups, Instant now) {
        String data = rollups.stream()
                .map(r -> "%s %s | h-%d | n=%d min=%s max=%s avg=%s".formatted(r.measure().name(), unit(r.measure()),
                        Math.max(0, Duration.between(r.hourStart(), now).toHours()), r.count(), fixed(r.min()),
                        fixed(r.max()), fixed(r.avg())))
                .collect(Collectors.joining("\n"));
        return new Prompt(INSTRUCTIONS, data);
    }

    static String unit(Measure m) {
        return switch (m) {
            case SYSTOLIC, DIASTOLIC -> "mmHg";
            case PULSE -> "bpm";
            case GLUCOSE -> "mg/dL";
            case SPO2 -> "%";
            case WEIGHT -> "kg";
        };
    }

    private static String fixed(BigDecimal v) {
        return v.setScale(2, RoundingMode.HALF_UP).toPlainString();
    }
}
