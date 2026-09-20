package com.commercelab.order.messaging;

import static org.assertj.core.api.Assertions.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.listener.ContainerProperties;

class ConsumerConfigurationTest {
    @Test void containerPinsNoSkipSettingsEvenWhenBootPropertiesConflict() {
        var kafka = new KafkaProperties();
        kafka.getConsumer().setEnableAutoCommit(true);
        kafka.getConsumer().setMaxPollRecords(100);
        var config = new ConsumerConfiguration();
        var consumer = config.workflowConsumerFactory(kafka);
        assertThat(consumer.getConfigurationProperties())
                .containsEntry("group.id", "commerce-order-inventory-result-v1")
                .containsEntry("enable.auto.commit", false)
                .containsEntry("auto.offset.reset", "earliest")
                .containsEntry("key.deserializer", StringDeserializer.class)
                .containsEntry("value.deserializer", StringDeserializer.class)
                .containsEntry("max.poll.records", 1)
                .containsEntry("max.poll.interval.ms", 300000)
                .containsEntry("session.timeout.ms", 10000)
                .containsEntry("heartbeat.interval.ms", 3000);
        try (var failures = new PartitionFailureHandler(new ConsumerMetrics(new SimpleMeterRegistry()))) {
            var factory = config.workflowKafkaListenerContainerFactory(consumer, failures);
            var container = factory.createContainer(ConsumerConfiguration.TOPIC);
            assertThat(container.getConcurrency()).isEqualTo(3);
            var settings = container.getContainerProperties();
            assertThat(settings.getAckMode()).isEqualTo(ContainerProperties.AckMode.RECORD);
            assertThat(settings.isSyncCommits()).isTrue();
            assertThat(settings.getPollTimeout()).isEqualTo(1000);
            assertThat(settings.getKafkaAwareTransactionManager()).isNull();
            assertThat(settings.getConsumerRebalanceListener()).isNotNull();
        }
    }
}
