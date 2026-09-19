package com.commercelab.order.outbox;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;

class OutboxConfigurationTest {
    final OutboxDeliveryStore store = mock(OutboxDeliveryStore.class);
    final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
            .withUserConfiguration(OutboxConfiguration.class)
            .withBean(OutboxDeliveryStore.class, () -> store)
            .withBean(OutboxRetryPolicy.class, OutboxRetryPolicy::new)
            .withBean(ObjectMapper.class, ObjectMapper::new)
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new);

    @ParameterizedTest
    @ValueSource(strings = {"max-attempts-per-pass=0", "max-attempts-per-pass=-1", "poll-interval-ms=0",
            "lease-ms=0", "ack-wait-ms=0", "max-block-ms=0", "delivery-timeout-ms=0", "request-timeout-ms=0",
            "lease-ms=14000", "ack-wait-ms=10000", "request-timeout-ms=10001"})
    void rejectsUnsafeBudgets(String property) {
        runner.withPropertyValues("order.outbox." + property).run(context -> assertThat(context).hasFailed());
    }

    @Test void disabledSchedulingHasNoTasksButManualRelayAndNoOpHookRemainAvailable() {
        runner.withPropertyValues("order.outbox.enabled=false").run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(OutboxRelay.class).hasSingleBean(OutboxPublicationHook.class);
            assertThat(context).doesNotHaveBean(ScheduledAnnotationBeanPostProcessor.class);
            assertThatCode(() -> context.getBean(OutboxPublicationHook.class).afterAcknowledgement(null)).doesNotThrowAnyException();
            verifyNoInteractions(store);
        });
    }

    @Test void enabledSchedulerRegistersOneTask() {
        runner.withPropertyValues("order.outbox.enabled=true", "order.outbox.poll-interval-ms=60000").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(ScheduledAnnotationBeanPostProcessor.class).getScheduledTasks()).hasSize(1);
        });
    }

    @Test void producerSettingsAndDiagnosticGaugesUseContract() {
        when(store.stats()).thenReturn(new OutboxStats(2, 17.5, 1));
        runner.withPropertyValues("order.outbox.enabled=false").run(context -> {
            var config = context.getBean(ProducerFactory.class).getConfigurationProperties();
            assertThat(config).containsEntry(ProducerConfig.ACKS_CONFIG, "all")
                    .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
                    .containsEntry(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1)
                    .containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000L)
                    .containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000)
                    .containsEntry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000)
                    .containsEntry(ProducerConfig.LINGER_MS_CONFIG, 0);
            var meters = context.getBean(MeterRegistry.class);
            assertThat(meters.get("outbox.pending").gauge().value()).isEqualTo(2);
            assertThat(meters.get("outbox.oldest.pending.age").gauge().value()).isEqualTo(17.5);
            assertThat(meters.get("outbox.failed.pending").gauge().value()).isEqualTo(1);
        });
    }
}
