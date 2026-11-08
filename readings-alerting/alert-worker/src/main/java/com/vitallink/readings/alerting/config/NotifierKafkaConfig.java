package com.vitallink.readings.alerting.config;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.vitallink.readings.alerting.notify.NotifierDeadLetters;
import com.vitallink.readings.messaging.KafkaTopics;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Containers for the notifier: per record, manual acks after the DB commit, cooperative-sticky. A failing record is
 * redelivered {@code max-retries} times with a fixed backoff (blocking only its partition; recording is one small
 * insert), then moved to {@code <topic>.dlq} on the same partition. Malformed records go to the DLQ at once. The
 * critical lane has its own factory and therefore its own consumers; it serves two containers in group
 * notifier-critical, one for alerts.raised.critical and one for alert.state_changed (D32).
 */
@Configuration
@Profile({"notifier", "notifier-critical"})
class NotifierKafkaConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> notifierListenerFactory(
            KafkaProperties kafka, ObjectProvider<SslBundles> sslBundles, NotifierProperties props,
            NotifierDeadLetters notifierDeadLetters) {
        return factory(kafka, sslBundles, props, notifierDeadLetters);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> notifierCriticalListenerFactory(
            KafkaProperties kafka, ObjectProvider<SslBundles> sslBundles, NotifierProperties props,
            NotifierDeadLetters notifierDeadLetters) {
        return factory(kafka, sslBundles, props, notifierDeadLetters);
    }

    @Bean
    NotifierDeadLetters notifierDeadLetters(KafkaTemplate<String, String> notifierDlqTemplate, MeterRegistry meters) {
        return new NotifierDeadLetters(new DeadLetterPublishingRecoverer(notifierDlqTemplate,
                (record, e) -> new TopicPartition(KafkaTopics.dlq(record.topic()), record.partition())), meters);
    }

    /** DLQ producer: acks=all and idempotent, so a moved record is not lost or doubled. */
    @Bean
    KafkaTemplate<String, String> notifierDlqTemplate(KafkaProperties kafka, ObjectProvider<SslBundles> sslBundles) {
        Map<String, Object> producer = kafka.buildProducerProperties(sslBundles.getIfAvailable());
        producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer.put(ProducerConfig.ACKS_CONFIG, "all");
        producer.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producer));
    }

    private static ConcurrentKafkaListenerContainerFactory<String, String> factory(
            KafkaProperties kafka, ObjectProvider<SslBundles> ssl, NotifierProperties props,
            NotifierDeadLetters deadLetters) {
        Map<String, Object> consumer = kafka.buildConsumerProperties(ssl.getIfAvailable());
        consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, List.of(CooperativeStickyAssignor.class));

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumer));
        factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        factory.setConcurrency(props.listenerConcurrency());
        DefaultErrorHandler errors = new DefaultErrorHandler(deadLetters,
                new FixedBackOff(props.retryInterval().toMillis(), props.maxRetries()));
        errors.addNotRetryableExceptions(JsonProcessingException.class, IllegalArgumentException.class);
        factory.setCommonErrorHandler(errors);
        return factory;
    }
}
