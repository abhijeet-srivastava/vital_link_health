package com.vitallink.readings.domain;

import java.time.Instant;
import java.util.Optional;

/** What one breach does to its episode. Only a new episode or a severity upgrade is announced. */
public sealed interface EpisodePlan {

    Optional<AlertChange.Reason> reason();

    /** The episode's severity after this breach. */
    Severity severity();

    boolean late();

    /** No unresolved episode: open one. escalateAt is set only for CRITICAL. */
    record Open(Severity severity, boolean late, Instant escalateAt) implements EpisodePlan {
        @Override
        public Optional<AlertChange.Reason> reason() {
            return Optional.of(AlertChange.Reason.RAISED);
        }
    }

    /**
     * Fold into the existing episode. When {@code upgraded}, severity, status and escalateAt are written as given;
     * otherwise only the counters, seen-times and late flag change (HOT update). {@code reopened}: an ACKNOWLEDGED
     * episode returned to OPEN because it rose to CRITICAL.
     */
    record Fold(long alertId, boolean upgraded, boolean reopened, Severity severity, AlertStatus status, boolean late,
                Instant escalateAt) implements EpisodePlan {
        @Override
        public Optional<AlertChange.Reason> reason() {
            return upgraded ? Optional.of(AlertChange.Reason.SEVERITY_UPGRADED) : Optional.empty();
        }
    }
}
