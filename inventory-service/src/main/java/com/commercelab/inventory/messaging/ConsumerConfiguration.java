package com.commercelab.inventory.messaging;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration(proxyBeanMethods = false)
@EnableKafka
@EnableConfigurationProperties(KafkaProperties.class)
public class ConsumerConfiguration {
    public static final String TOPIC = "commerce.orders.v1";
    public static final String GROUP = "commerce-inventory-order-placed-v1";

    @Bean
    ConsumerFactory<String, String> workflowConsumerFactory(KafkaProperties kafka) {
        var config = kafka.buildConsumerProperties(null);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> workflowKafkaListenerContainerFactory(
            ConsumerFactory<String, String> workflowConsumerFactory, PartitionFailureHandler failures) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(workflowConsumerFactory);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(failures);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setSyncCommits(true);
        factory.getContainerProperties().setPollTimeout(1000);
        factory.setContainerCustomizer(container -> container.getContainerProperties()
                .setConsumerRebalanceListener(failures.rebalanceListener(container)));
        return factory;
    }

    @Bean ConsumerMetrics consumerMetrics(MeterRegistry meters) { return new ConsumerMetrics(meters); }

    @Bean(destroyMethod = "close")
    PartitionFailureHandler partitionFailureHandler(ConsumerMetrics metrics) {
        return new PartitionFailureHandler(metrics);
    }

    @Bean
    @ConditionalOnMissingBean(ConsumerCommitHook.class)
    ConsumerCommitHook consumerCommitHook() { return (id, outcome) -> {}; }
}
