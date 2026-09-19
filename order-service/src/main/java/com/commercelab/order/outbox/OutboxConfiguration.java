package com.commercelab.order.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Gauge;
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
    OutboxRelay outboxRelay(OutboxDeliveryStore store, OutboxRetryPolicy retryPolicy, OutboxPublisher publisher,
            OutboxPublicationHook hook, OutboxProperties properties, MeterRegistry meters, ObjectMapper mapper) {
        Gauge.builder("outbox.pending", store, s -> s.stats().pendingCount()).register(meters);
        Gauge.builder("outbox.failed.pending", store, s -> s.stats().failedPendingCount()).register(meters);
        Gauge.builder("outbox.oldest.pending.age", store, s -> s.stats().oldestPendingAgeSeconds())
                .baseUnit("seconds").register(meters);
        return new OutboxRelay(store, retryPolicy, publisher, hook, properties, meters, mapper);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "order.outbox.enabled", havingValue = "true", matchIfMissing = true)
    @EnableScheduling
    static class PollingConfiguration {
        @Bean OutboxPollingTask outboxPollingTask(OutboxRelay relay) { return new OutboxPollingTask(relay); }
    }

    static class OutboxPollingTask {
        private final OutboxRelay relay;
        OutboxPollingTask(OutboxRelay relay) { this.relay = relay; }

        @Scheduled(fixedDelayString = "${order.outbox.poll-interval-ms:1000}",
                initialDelayString = "${order.outbox.poll-interval-ms:1000}")
        public void poll() { relay.runOnce(); }
    }
}
