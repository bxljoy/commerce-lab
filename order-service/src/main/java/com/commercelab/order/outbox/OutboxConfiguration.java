package com.commercelab.order.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({OutboxProperties.class, KafkaProperties.class})
public class OutboxConfiguration {
    @Bean
    ProducerFactory<String, String> outboxProducerFactory(KafkaProperties kafka, OutboxProperties outbox) {
        var config = kafka.buildProducerProperties(null);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, outbox.maxBlockMs());
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, outbox.deliveryTimeoutMs());
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, outbox.requestTimeoutMs());
        config.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    KafkaTemplate<String, String> outboxKafkaTemplate(ProducerFactory<String, String> factory) {
        var template = new KafkaTemplate<>(factory);
        // The default listener includes key/value in error logs. Relay logs only stable metadata.
        template.setProducerListener(new org.springframework.kafka.support.ProducerListener<>() {});
        return template;
    }

    @Bean
    OutboxPublisher outboxPublisher(KafkaTemplate<String, String> template, OutboxProperties properties) {
        return new KafkaOutboxPublisher(template, properties);
    }

    @Bean
    @ConditionalOnMissingBean(OutboxPublicationHook.class)
    OutboxPublicationHook outboxPublicationHook() { return message -> {}; }

    @Bean
    OutboxMetrics outboxMetrics(OutboxDeliveryStore store, MeterRegistry meters) {
        return new OutboxMetrics(store, meters);
    }

    @Bean
    OutboxRelay outboxRelay(OutboxDeliveryStore store, OutboxRetryPolicy retryPolicy, OutboxPublisher publisher,
            OutboxPublicationHook hook, OutboxProperties properties, OutboxMetrics metrics, ObjectMapper mapper) {
        return new OutboxRelay(store, retryPolicy, publisher, hook, properties, metrics, mapper);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "order.outbox.enabled", havingValue = "true", matchIfMissing = true)
    @EnableScheduling
    static class PollingConfiguration {
        @Bean OutboxPollingTask outboxPollingTask(OutboxRelay relay, OutboxMetrics metrics) {
            return new OutboxPollingTask(relay, metrics);
        }
    }

    static class OutboxPollingTask {
        private final OutboxRelay relay;
        private final OutboxMetrics metrics;
        OutboxPollingTask(OutboxRelay relay, OutboxMetrics metrics) {
            this.relay = relay;
            this.metrics = metrics;
        }

        @Scheduled(fixedDelayString = "${order.outbox.poll-interval-ms:1000}",
                initialDelayString = "${order.outbox.poll-interval-ms:1000}")
        public void poll() { relay.runOnce(); }

        @Scheduled(fixedDelayString = "${order.outbox.poll-interval-ms:1000}",
                initialDelayString = "${order.outbox.poll-interval-ms:1000}")
        public void refreshMetrics() { metrics.refresh(); }
    }
}
