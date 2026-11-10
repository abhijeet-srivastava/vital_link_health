package com.vitallink.readings.ingest.backpressure;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.vitallink.readings.persistence.JdbcOutboxBacklog;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

@Configuration
@EnableConfigurationProperties(BackpressureProperties.class)
public class BackpressureConfig {

    @Bean
    OutboxBacklogMonitor outboxBacklogMonitor(JdbcClient jdbc, BackpressureProperties props) {
        return new OutboxBacklogMonitor(new JdbcOutboxBacklog(jdbc), props);
    }

    @Bean
    InFlightLimiter inFlightLimiter(OutboxBacklogMonitor backlog, BackpressureProperties props) {
        return new InFlightLimiter(backlog, props);
    }

    @Bean
    DeviceRateLimiter deviceRateLimiter(StringRedisTemplate redis, CircuitBreakerRegistry breakers,
                                        BackpressureProperties props) {
        return new DeviceRateLimiter(redis, breakers, props);
    }
}
