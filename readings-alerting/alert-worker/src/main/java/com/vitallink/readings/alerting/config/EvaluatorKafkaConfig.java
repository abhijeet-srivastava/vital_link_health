package com.vitallink.readings.alerting.config;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.vitallink.readings.messaging.KafkaTopics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Container and retries for the evaluator. Manual acks after the DB commit; cooperative-sticky assignment (a
 * rebalance does not stop every partition). Retries are non-blocking: a failing record is republished to
 * {@code readings.ingested.retry} and its main partition moves on at once, so one bad event cannot hold back a
 * CRITICAL reading for another patient on the same partition. The retry topic is consumed by the same listener after
 * a fixed delay; after the last attempt the record goes to {@code readings.ingested.dlq}. Malformed records go to the
 * DLQ without retries.
 *
 * <p>Trade-off: a retried event leaves its patient's ordering. Episode folding does not depend on arrival order
 * (LEAST/GREATEST seen-times, severity only rises), so the alert ends up the same; only which change is announced
 * as RAISED versus SEVERITY_UPGRADED can differ.
 */
@Configuration
@Profile("evaluator")
class EvaluatorKafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(EvaluatorKafkaConfig.class);

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> evaluatorListenerFactory(
            KafkaProperties kafka, ObjectProvider<SslBundles> sslBundles, AlertingProperties props) {
        Map<String, Object> consumer = kafka.buildConsumerProperties(sslBundles.getIfAvailable());
        consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, List.of(CooperativeStickyAssignor.class));

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumer));
        factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        factory.setConcurrency(props.listenerConcurrency());
        return factory;   // the retry-topic configurer installs the error handler that republishes failures
    }

    @Bean
    RetryTopicConfiguration readingsIngestedRetryTopic(KafkaTemplate<String, String> dlqTemplate,
                                                       AlertingProperties props) {
        return RetryTopicConfigurationBuilder.newInstance()
                .includeTopic(KafkaTopics.READINGS_INGESTED)
                .maxAttempts(props.maxRetries() + 1)
                .fixedBackOff(props.retryInterval().toMillis())
                .sameIntervalTopicReuseStrategy(SameIntervalTopicReuseStrategy.SINGLE_TOPIC)   // one .retry topic
                .retryTopicSuffix(".retry")
                .dltSuffix(".dlq")
                .doNotAutoCreateRetryTopics()                      // TopicsConfig creates them with the main counts
                .notRetryOn(List.of(JsonProcessingException.class, IllegalArgumentException.class))
                .listenerFactory("evaluatorListenerFactory")
                .dltHandlerMethod("evaluatorDeadLetters", "onDeadLetter")
                .create(dlqTemplate);
    }

    /** Backoff on the retry topic pauses and resumes its partitions on this scheduler. */
    @Bean
    TaskScheduler retryTopicTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix("evaluator-retry-");
        scheduler.setPoolSize(1);
        return scheduler;
    }

    @Bean
    EvaluatorDeadLetters evaluatorDeadLetters(MeterRegistry meters) {
        return new EvaluatorDeadLetters(meters);
    }

    /** DLQ producer: acks=all and idempotent, so a retried or dead-lettered record is not lost or doubled. */
    @Bean
    KafkaTemplate<String, String> dlqTemplate(KafkaProperties kafka, ObjectProvider<SslBundles> sslBundles) {
        Map<String, Object> producer = kafka.buildProducerProperties(sslBundles.getIfAvailable());
        producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer.put(ProducerConfig.ACKS_CONFIG, "all");
        producer.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producer));
    }

    /** Sees each record that reached the DLQ: counts it and logs coordinates only (payloads hold reading values). */
    static class EvaluatorDeadLetters {

        private final Counter deadLettered;

        EvaluatorDeadLetters(MeterRegistry meters) {
            this.deadLettered = Counter.builder("vitallink.evaluator.dlq")
                    .description("readings.ingested records that reached the DLQ").register(meters);
        }

        public void onDeadLetter(ConsumerRecord<String, String> record) {
            deadLettered.increment();
            log.warn("Dead-lettered topic={} partition={} offset={} error={}", record.topic(), record.partition(),
                    record.offset(), exceptionClass(record));
        }

        /** Root-cause class name from the recoverer's headers (kafka_dlt-... or kafka_... depending on the path). */
        static String exceptionClass(ConsumerRecord<String, String> record) {
            for (String suffix : List.of("exception-cause-fqcn", "exception-fqcn")) {
                Header found = null;
                for (Header h : record.headers()) {
                    if (h.key().endsWith(suffix)) {
                        found = h;
                    }
                }
                if (found != null && found.value() != null) {
                    return new String(found.value(), StandardCharsets.UTF_8);
                }
            }
            return "unknown";
        }
    }
}
