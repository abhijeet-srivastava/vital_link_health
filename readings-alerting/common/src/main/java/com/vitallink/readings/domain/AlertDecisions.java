package com.vitallink.readings.domain;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The alert rules from references/alert-processing.md, free of I/O: which band fires, which bands are in force, and
 * what a breach does to its episode. Severity is rule-derived only (rule 8).
 */
public class AlertDecisions {

    /** Most severe first; equal severities by threshold_id so the recorded rule is deterministic. */
    private static final Comparator<ThresholdBand> MOST_SEVERE_FIRST =
            Comparator.comparingInt((ThresholdBand b) -> b.severity().rank()).thenComparingLong(ThresholdBand::thresholdId);

    private final Clock clock;
    private final boolean reopenOnCritical;
    private final Duration escalationWindow;
    private final Duration lateAfter;

    /**
     * @param reopenOnCritical open clinical decision: an ACKNOWLEDGED alert that rises to CRITICAL returns to OPEN
     * @param escalationWindow time from OPEN + CRITICAL to escalation (15 minutes by default)
     * @param lateAfter        a breach measured longer ago than this still alerts, flagged is_late (7 days)
     */
    public AlertDecisions(Clock clock, boolean reopenOnCritical, Duration escalationWindow, Duration lateAfter) {
        this.clock = clock;
        this.reopenOnCritical = reopenOnCritical;
        this.escalationWindow = escalationWindow;
        this.lateAfter = lateAfter;
    }

    /** The most severe band the value crosses; empty when it lies inside every band. */
    public static Optional<ThresholdBand> mostSevere(List<ThresholdBand> bands, BigDecimal value) {
        return bands.stream().filter(b -> b.crossedBy(value)).min(MOST_SEVERE_FIRST);
    }

    /**
     * Bands in force for one patient. A patient override replaces every program default for that measure. Without
     * one, the policy decides between the programs (given in enrollment order).
     */
    public static Map<Measure, List<ThresholdBand>> effectiveBands(List<ThresholdBand> overrides,
                                                                   List<List<ThresholdBand>> programDefaults,
                                                                   MultiProgramPolicy policy) {
        Map<Measure, List<ThresholdBand>> effective = new EnumMap<>(Measure.class);
        for (List<ThresholdBand> program : programDefaults) {
            byMeasure(program).forEach((measure, bands) -> {
                if (policy == MultiProgramPolicy.MOST_CONSERVATIVE) {
                    // Union: the most severe band crossed across all programs wins at evaluation time.
                    effective.merge(measure, bands, AlertDecisions::concat);
                } else {
                    effective.putIfAbsent(measure, bands);
                }
            });
        }
        effective.putAll(byMeasure(overrides));
        return effective;
    }

    public boolean isLate(Instant measuredAt) {
        return measuredAt.isBefore(clock.instant().minus(lateAfter));
    }

    /** New episode, fold, or upgrade in place (with reopen and the escalation timer when it applies). */
    public EpisodePlan plan(BreachWithReading breach, Optional<ActiveEpisode> current) {
        Severity severity = breach.breach().severity();
        boolean late = isLate(breach.measuredAt());
        if (current.isEmpty()) {
            return new EpisodePlan.Open(severity, late, timerFor(AlertStatus.OPEN, severity));
        }
        ActiveEpisode episode = current.get();
        boolean stillLate = episode.late() && late;
        if (!severity.moreSevereThan(episode.severity())) {
            return new EpisodePlan.Fold(episode.alertId(), false, false, episode.severity(), episode.status(), stillLate,
                    null);
        }
        boolean reopen = reopenOnCritical && severity == Severity.CRITICAL
                && episode.status() == AlertStatus.ACKNOWLEDGED;
        AlertStatus status = reopen ? AlertStatus.OPEN : episode.status();
        return new EpisodePlan.Fold(episode.alertId(), true, reopen, severity, status, stillLate,
                timerFor(status, severity));
    }

    /** escalate_at exists only on OPEN + CRITICAL; the CHECK on alert rejects anything else. */
    private Instant timerFor(AlertStatus status, Severity severity) {
        return status == AlertStatus.OPEN && severity == Severity.CRITICAL
                ? clock.instant().plus(escalationWindow)
                : null;
    }

    private static Map<Measure, List<ThresholdBand>> byMeasure(Collection<ThresholdBand> bands) {
        return bands.stream().collect(Collectors.groupingBy(ThresholdBand::measure,
                () -> new EnumMap<>(Measure.class), Collectors.toUnmodifiableList()));
    }

    private static List<ThresholdBand> concat(List<ThresholdBand> a, List<ThresholdBand> b) {
        return Stream.concat(a.stream(), b.stream()).toList();
    }
}
