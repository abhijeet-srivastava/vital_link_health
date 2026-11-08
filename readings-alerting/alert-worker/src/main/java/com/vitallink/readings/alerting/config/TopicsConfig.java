package com.vitallink.readings.alerting.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import com.vitallink.readings.messaging.KafkaTopics;

/**
 * Creates every topic and its DLQ on startup (Spring's KafkaAdmin; existing topics are left as they are). Broker
 * auto-create is off, so a missing topic fails loudly; {@code spring.kafka.admin.fail-fast} stops start-up if the
 * topics cannot be created.
 */
@Configuration
class TopicsConfig {

    @Bean
    KafkaAdmin.NewTopics vitallinkTopics(@Value("${vitallink.kafka.replication-factor}") short replicationFactor) {
        String minInsync = Integer.toString(Math.min(2, replicationFactor));
        return new KafkaAdmin.NewTopics(KafkaTopics.all().stream()
                .map(t -> TopicBuilder.name(t.name())
                        .partitions(t.partitions())
                        .replicas(replicationFactor)
                        .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInsync)
                        .build())
                .toArray(NewTopic[]::new));
    }
}
