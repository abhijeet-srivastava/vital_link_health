package com.vitallink.readings.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.vitallink.readings.domain.ThresholdBand.Operator;

class AlertDecisionsTest {

    static final Instant NOW = Instant.parse("2026-11-08T06:00:00Z");
    static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    static final Duration WINDOW = Duration.ofMinutes(15);

    static final ThresholdBand SYS_HIGH = band(1, Measure.SYSTOLIC, Operator.GT, 160, Severity.HIGH);
    static final ThresholdBand SYS_CRITICAL = band(2, Measure.SYSTOLIC, Operator.GT, 180, Severity.CRITICAL);
    static final ThresholdBand SYS_LOW = band(3, Measure.SYSTOLIC, Operator.LT, 90, Severity.CRITICAL);

    final AlertDecisions decisions = decisions(true);

    @Test
    void most_severe_band_wins() {
        List<ThresholdBand> bands = List.of(SYS_HIGH, SYS_CRITICAL, SYS_LOW);

        assertThat(AlertDecisions.mostSevere(bands, BigDecimal.valueOf(186))).contains(SYS_CRITICAL);
        assertThat(AlertDecisions.mostSevere(bands, BigDecimal.valueOf(170))).contains(SYS_HIGH);
        assertThat(AlertDecisions.mostSevere(bands, BigDecimal.valueOf(80))).contains(SYS_LOW);
    }

    @Test
    void value_inside_every_band_gives_no_breach() {
        List<ThresholdBand> bands = List.of(SYS_HIGH, SYS_CRITICAL, SYS_LOW);

        assertThat(AlertDecisions.mostSevere(bands, BigDecimal.valueOf(120))).isEmpty();
        assertThat(AlertDecisions.mostSevere(bands, BigDecimal.valueOf(160))).as("limit itself is inside").isEmpty();
        assertThat(AlertDecisions.mostSevere(List.of(), BigDecimal.valueOf(300))).isEmpty();
    }

    @Test
    void patient_override_replaces_program_default() {
        ThresholdBand override = band(10, Measure.SYSTOLIC, Operator.GT, 200, Severity.HIGH);
        ThresholdBand programDiastolic = band(11, Measure.DIASTOLIC, Operator.GT, 100, Severity.HIGH);

        Map<Measure, List<ThresholdBand>> effective = AlertDecisions.effectiveBands(List.of(override),
                List.of(List.of(SYS_HIGH, SYS_CRITICAL, programDiastolic)), MultiProgramPolicy.MOST_CONSERVATIVE);

        assertThat(effective.get(Measure.SYSTOLIC)).containsExactly(override);
        assertThat(effective.get(Measure.DIASTOLIC)).as("measures without override keep the default")
                .containsExactly(programDiastolic);
    }

    @Test
    void multi_program_takes_most_conservative() {
        ThresholdBand hypertensionGlucose = band(20, Measure.GLUCOSE, Operator.GT, 250, Severity.HIGH);
        ThresholdBand diabetesGlucose = band(21, Measure.GLUCOSE, Operator.GT, 180, Severity.MEDIUM);
        ThresholdBand diabetesGlucoseHigh = band(22, Measure.GLUCOSE, Operator.GT, 200, Severity.HIGH);

        Map<Measure, List<ThresholdBand>> effective = AlertDecisions.effectiveBands(List.of(),
                List.of(List.of(hypertensionGlucose), List.of(diabetesGlucose, diabetesGlucoseHigh)),
                MultiProgramPolicy.MOST_CONSERVATIVE);

        List<ThresholdBand> glucose = effective.get(Measure.GLUCOSE);
        assertThat(AlertDecisions.mostSevere(glucose, BigDecimal.valueOf(190))).contains(diabetesGlucose);
        assertThat(AlertDecisions.mostSevere(glucose, BigDecimal.valueOf(210))).contains(diabetesGlucoseHigh);
    }

    @Test
    void multi_program_flag_off_uses_first_enrolled_program() {
        ThresholdBand first = band(20, Measure.GLUCOSE, Operator.GT, 250, Severity.HIGH);
        ThresholdBand second = band(21, Measure.GLUCOSE, Operator.GT, 180, Severity.MEDIUM);
        ThresholdBand onlySecondHasSpo2 = band(23, Measure.SPO2, Operator.LT, 92, Severity.HIGH);

        Map<Measure, List<ThresholdBand>> effective = AlertDecisions.effectiveBands(List.of(),
                List.of(List.of(first), List.of(second, onlySecondHasSpo2)), MultiProgramPolicy.FIRST_ENROLLED);

        assertThat(effective.get(Measure.GLUCOSE)).containsExactly(first);
        assertThat(effective.get(Measure.SPO2)).containsExactly(onlySecondHasSpo2);
    }

    @Test
    void first_breach_opens_episode_and_critical_sets_escalate_at() {
        EpisodePlan high = decisions.plan(breach(SYS_HIGH, 170, NOW), Optional.empty());
        EpisodePlan critical = decisions.plan(breach(SYS_CRITICAL, 186, NOW), Optional.empty());

        assertThat(high).isEqualTo(new EpisodePlan.Open(Severity.HIGH, false, null));
        assertThat(critical).isEqualTo(new EpisodePlan.Open(Severity.CRITICAL, false, NOW.plus(WINDOW)));
        assertThat(critical.reason()).contains(AlertChange.Reason.RAISED);
    }

    @Test
    void repeat_same_severity_folds_without_event() {
        ActiveEpisode open = new ActiveEpisode(7, AlertStatus.OPEN, Severity.HIGH, false);

        EpisodePlan plan = decisions.plan(breach(SYS_HIGH, 165, NOW), Optional.of(open));

        assertThat(plan).isInstanceOf(EpisodePlan.Fold.class);
        EpisodePlan.Fold fold = (EpisodePlan.Fold) plan;
        assertThat(fold.upgraded()).isFalse();
        assertThat(fold.alertId()).isEqualTo(7);
        assertThat(plan.reason()).isEmpty();
    }

    @Test
    void lower_severity_folds_without_event() {
        ActiveEpisode critical = new ActiveEpisode(7, AlertStatus.OPEN, Severity.CRITICAL, false);

        EpisodePlan plan = decisions.plan(breach(SYS_HIGH, 165, NOW), Optional.of(critical));

        assertThat(((EpisodePlan.Fold) plan).upgraded()).isFalse();
        assertThat(((EpisodePlan.Fold) plan).severity()).isEqualTo(Severity.CRITICAL);
        assertThat(plan.reason()).isEmpty();
    }

    @Test
    void high_then_critical_upgrades_and_sets_escalate_at() {
        ActiveEpisode high = new ActiveEpisode(7, AlertStatus.OPEN, Severity.HIGH, false);

        EpisodePlan plan = decisions.plan(breach(SYS_CRITICAL, 190, NOW), Optional.of(high));

        assertThat(plan).isEqualTo(new EpisodePlan.Fold(7, true, false, Severity.CRITICAL, AlertStatus.OPEN, false,
                NOW.plus(WINDOW)));
        assertThat(plan.reason()).contains(AlertChange.Reason.SEVERITY_UPGRADED);
    }

    @Test
    void upgrade_to_critical_on_acknowledged_reopens_when_flag_on() {
        ActiveEpisode acknowledged = new ActiveEpisode(7, AlertStatus.ACKNOWLEDGED, Severity.HIGH, false);

        EpisodePlan.Fold fold = (EpisodePlan.Fold) decisions.plan(breach(SYS_CRITICAL, 190, NOW),
                Optional.of(acknowledged));

        assertThat(fold.status()).isEqualTo(AlertStatus.OPEN);
        assertThat(fold.reopened()).isTrue();
        assertThat(fold.escalateAt()).isEqualTo(NOW.plus(WINDOW));
        assertThat(fold.reason()).contains(AlertChange.Reason.SEVERITY_UPGRADED);
    }

    @Test
    void upgrade_to_critical_on_acknowledged_stays_acknowledged_when_flag_off() {
        ActiveEpisode acknowledged = new ActiveEpisode(7, AlertStatus.ACKNOWLEDGED, Severity.HIGH, false);

        EpisodePlan.Fold fold = (EpisodePlan.Fold) decisions(false).plan(breach(SYS_CRITICAL, 190, NOW),
                Optional.of(acknowledged));

        assertThat(fold.status()).isEqualTo(AlertStatus.ACKNOWLEDGED);
        assertThat(fold.reopened()).isFalse();
        assertThat(fold.severity()).isEqualTo(Severity.CRITICAL);
        assertThat(fold.escalateAt()).as("timer only on OPEN + CRITICAL").isNull();
        assertThat(fold.reason()).contains(AlertChange.Reason.SEVERITY_UPGRADED);
    }

    @Test
    void upgrade_below_critical_sets_no_timer() {
        ThresholdBand medium = band(4, Measure.SYSTOLIC, Operator.GT, 150, Severity.MEDIUM);
        ActiveEpisode open = new ActiveEpisode(7, AlertStatus.OPEN, medium.severity(), false);

        EpisodePlan.Fold fold = (EpisodePlan.Fold) decisions.plan(breach(SYS_HIGH, 170, NOW), Optional.of(open));

        assertThat(fold.upgraded()).isTrue();
        assertThat(fold.escalateAt()).isNull();
    }

    @Test
    void reading_older_than_7_days_is_late() {
        assertThat(decisions.isLate(NOW.minus(Duration.ofDays(7)).minusSeconds(1))).isTrue();
        assertThat(decisions.isLate(NOW.minus(Duration.ofDays(7)).plusSeconds(1))).isFalse();

        EpisodePlan plan = decisions.plan(breach(SYS_HIGH, 170, NOW.minus(Duration.ofDays(8))), Optional.empty());
        assertThat(((EpisodePlan.Open) plan).late()).isTrue();
    }

    @Test
    void fresh_breach_clears_late_flag_on_fold() {
        ActiveEpisode lateEpisode = new ActiveEpisode(7, AlertStatus.OPEN, Severity.HIGH, true);

        EpisodePlan.Fold fold = (EpisodePlan.Fold) decisions.plan(breach(SYS_HIGH, 170, NOW), Optional.of(lateEpisode));

        assertThat(fold.late()).isFalse();
    }

    static AlertDecisions decisions(boolean reopenOnCritical) {
        return new AlertDecisions(CLOCK, reopenOnCritical, WINDOW, Duration.ofDays(7));
    }

    static ThresholdBand band(long id, Measure measure, Operator op, int limit, Severity severity) {
        return new ThresholdBand(id, measure, op, BigDecimal.valueOf(limit), severity);
    }

    static BreachWithReading breach(ThresholdBand band, int value, Instant measuredAt) {
        return new BreachWithReading(42, 9, UUID.randomUUID(), measuredAt,
                new Breach(band.measure(), BigDecimal.valueOf(value), band));
    }
}
