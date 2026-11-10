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
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.vitallink.readings.alerting.evaluator.EvaluatorDeadLetters;
import com.vitallink.readings.messaging.KafkaTopics;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Containers for the evaluator. Manual acks after the DB commit; cooperative-sticky assignment (a rebalance does not
 * stop every partition).
 *
 * <ul>
 *   <li>readings.ingested: batch listener, up to {@code batch-max-records} per poll; the broker holds a fetch up to
 *       {@code batch-max-wait} until {@code batch-min-bytes} are there, so batches fill under load and a lone
 *       CRITICAL on a quiet partition waits at most that long. Failing events are moved to the retry topic by the
 *       listener (non-blocking retries are not available for batch listeners in spring-kafka, D29); an exception that
 *       still escapes redelivers the whole poll with backoff and is never dead-lettered.</li>
 *   <li>readings.ingested.retry: per record, fixed backoff, then readings.ingested.dlq. Malformed records go to the
 *       DLQ at once. The backoff holds only the retry partition.</li>
 *   <li>thresholds.changed: per pod, latest offset, no retries (cache eviction only).</li>
 * </ul>
 */
@Configuration
@Profile("evaluator")
class EvaluatorKafkaConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> evaluatorBatchListenerFactory(
            KafkaProperties kafka, ObjectProvider<SslBundles> sslBundles, AlertingProperties props) {
        Map<String, Object> consumer = consumerProperties(kafka, sslBundles);
        consumer.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, props.batchMaxRecords());
        consumer.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, (int) props.batchMaxWait().toMillis());
        consumer.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, props.batchMinBytes());

        ConcurrentKafkaListenerContainerFactory<String, String> factory = factory(consumer, props.listenerConcurrency());
        factory.setBatchListener(true);
        ExponentialBackOff backOff = new ExponentialBackOff(500, 2);
        backOff.setMaxInterval(10_000);   // no max elapsed time: redeliver until the broker is back
        factory.setCommonErrorHandler(new DefaultErrorHandler(backOff));
        return factory;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> evaluatorRetryListenerFactory(
            KafkaProperties kafka, ObjectProvider<SslBundles> sslBundles, AlertingProperties props,
            EvaluatorDeadLetters deadLetters) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                factory(consumerProperties(kafka, sslBundles), props.listenerConcurrency());
        // First attempt on arrival, then maxRetries - 1 more: with the failed attempt on the main topic, every event
        // gets 1 + maxRetries attempts before the DLQ, as before.
        DefaultErrorHandler errors = new DefaultErrorHandler(deadLetters,
                new FixedBackOff(props.retryInterval().toMillis(), Math.max(0, props.maxRetries() - 1)));
        errors.addNotRetryableExceptions(JsonProcessingException.class, IllegalArgumentException.class);
        factory.setCommonErrorHandler(errors);
        return factory;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> thresholdsListenerFactory(
            KafkaProperties kafka, ObjectProvider<SslBundles> sslBundles) {
        Map<String, Object> consumer = consumerProperties(kafka, sslBundles);
        consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        ConcurrentKafkaListenerContainerFactory<String, String> factory = factory(consumer, 1);
        factory.getContainerProperties().setAckMode(AckMode.RECORD);
        return factory;
    }

    @Bean
    EvaluatorDeadLetters evaluatorDeadLetters(KafkaTemplate<String, String> dlqTemplate, MeterRegistry meters) {
        // Same partition number: the DLQ and the retry topic have the main topic's partition count.
        String dlq = KafkaTopics.dlq(KafkaTopics.READINGS_INGESTED);
        return new EvaluatorDeadLetters(new DeadLetterPublishingRecoverer(dlqTemplate,
                (record, e) -> new TopicPartition(dlq, record.partition())), meters);
    }

    /** Retry-topic and DLQ producer: acks=all and idempotent, so a moved record is not lost or doubled. */
    @Bean
    KafkaTemplate<String, String> dlqTemplate(KafkaProperties kafka, ObjectProvider<SslBundles> sslBundles) {
        Map<String, Object> producer = kafka.buildProducerProperties(sslBundles.getIfAvailable());
        producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer.put(ProducerConfig.ACKS_CONFIG, "all");
        producer.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producer));
    }

    private static Map<String, Object> consumerProperties(KafkaProperties kafka, ObjectProvider<SslBundles> ssl) {
        Map<String, Object> consumer = kafka.buildConsumerProperties(ssl.getIfAvailable());
        consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, List.of(CooperativeStickyAssignor.class));
        return consumer;
    }

    private static ConcurrentKafkaListenerContainerFactory<String, String> factory(Map<String, Object> consumer,
                                                                                  int concurrency) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumer));
        factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        factory.setConcurrency(concurrency);
        return factory;
    }
}
