package com.vitallink.readings.persistence;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * Prod profile only: refuses to start while threshold rules from the dev seed exist. Those bands have no clinical
 * sign-off (open clinical decision in references/decisions.md), and severity is derived from them. Runs while the
 * context refreshes, so a refused app never opens its port or consumes a message.
 */
@Component
@Profile("prod")
public class SeedThresholdGuard implements InitializingBean {

    static final String SEED_LABEL = "SEED-NOT-CLINICAL";

    private final JdbcClient jdbc;

    public SeedThresholdGuard(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void afterPropertiesSet() {
        long seedRules = jdbc.sql("SELECT count(*) FROM threshold_rule WHERE created_by = ?")
                .param(SEED_LABEL)
                .query(Long.class)
                .single();
        if (seedRules > 0) {
            throw new IllegalStateException("Refusing to start with the prod profile: " + seedRules
                    + " threshold_rule rows have created_by = '" + SEED_LABEL + "' (dev seed bands without clinical"
                    + " sign-off). Replace them with clinician-approved rules.");
        }
    }
}
