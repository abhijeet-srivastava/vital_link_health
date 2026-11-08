package com.vitallink.readings.domain;

import java.util.List;

/** A patient's current care team from care_assignment (valid_to IS NULL). primary is null when none is open. */
public record CareTeam(Long primary, List<Long> backups) {
}
