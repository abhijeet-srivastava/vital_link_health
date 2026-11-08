package com.vitallink.readings.alerting.config;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vitallink.readings.messaging.UuidV7;

/** Beans every profile uses (evaluator, scheduler); tests replace the clock with a fixed one. */
@Configuration
class CoreConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    UuidV7 uuidV7(Clock clock) {
        return new UuidV7(clock);
    }
}
