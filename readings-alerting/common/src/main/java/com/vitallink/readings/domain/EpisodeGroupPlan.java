package com.vitallink.readings.domain;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Every breach of one batch for one (patient, measure), folded in order with {@link AlertDecisions#plan}: the same
 * steps the per-event path would take, so the events and audit rows match it, while the database sees one INSERT
 * or one UPDATE row with the final state.
 *
 * @param before the locked episode, empty when the first step opens one
 * @param steps  one per breach, in evaluation order
 */
public record EpisodeGroupPlan(Optional<ActiveEpisode> before, List<Step> steps) {

    /** alert_id stand-in in the steps after an Open: the real id exists only after the INSERT. */
    public static final long NEW_EPISODE = 0;

    public record Step(BreachWithReading breach, EpisodePlan plan) {
    }

    public EpisodeGroupPlan {
        if (steps.isEmpty()) {
            throw new IllegalArgumentException("An episode plan needs at least one breach");
        }
        steps = List.copyOf(steps);
    }

    public EpisodeKey key() {
        return EpisodeKey.of(steps.get(0).breach());
    }

    public boolean opensEpisode() {
        return before.isEmpty();
    }

    /** Severity rose on an existing episode: the UPDATE writes severity, status and escalate_at too. */
    public boolean upgraded() {
        return steps.stream().anyMatch(s -> s.plan() instanceof EpisodePlan.Fold f && f.upgraded());
    }

    public int count() {
        return steps.size();
    }

    public Severity severity() {
        return last().severity();
    }

    public AlertStatus status() {
        return last() instanceof EpisodePlan.Fold f ? f.status() : AlertStatus.OPEN;
    }

    public boolean late() {
        return last().late();
    }

    /** The timer from the last step that set one (open or upgrade); later repeat breaches leave it alone. */
    public Instant escalateAt() {
        for (int i = steps.size() - 1; i >= 0; i--) {
            switch (steps.get(i).plan()) {
                case EpisodePlan.Open open -> {
                    return open.escalateAt();
                }
                case EpisodePlan.Fold fold when fold.upgraded() -> {
                    return fold.escalateAt();
                }
                case EpisodePlan.Fold fold -> {
                }
            }
        }
        return null;
    }

    public Instant firstSeen() {
        return steps.stream().map(s -> s.breach().measuredAt()).min(Comparator.naturalOrder()).orElseThrow();
    }

    public Instant lastSeen() {
        return steps.stream().map(s -> s.breach().measuredAt()).max(Comparator.naturalOrder()).orElseThrow();
    }

    private EpisodePlan last() {
        return steps.get(steps.size() - 1).plan();
    }
}
