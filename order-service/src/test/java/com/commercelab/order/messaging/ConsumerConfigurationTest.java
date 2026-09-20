package com.commercelab.order.messaging;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.List;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentCaptor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.util.ReflectionTestUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.listener.ContainerProperties;

class ConsumerConfigurationTest {
    @Test void enabledListenersDeferStartupAndDisabledListenersHaveNoRetryWorker() throws Exception {
        var annotation = InventoryResultListener.class.getMethod("onRecord", ConsumerRecord.class)
                .getAnnotation(org.springframework.kafka.annotation.KafkaListener.class);
        assertThat(annotation.autoStartup()).isEqualTo("false");
        var runner = new org.springframework.boot.test.context.runner.ApplicationContextRunner()
                .withUserConfiguration(ConsumerConfiguration.class)
                .withBean(io.micrometer.core.instrument.MeterRegistry.class, SimpleMeterRegistry::new);
        runner.run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(WorkflowConsumerLifecycle.class);
            assertThat(context.getBean(WorkflowConsumerLifecycle.class).isRunning()).isFalse();
        });
        runner.withPropertyValues("order.events.enabled=false").run(context ->
                assertThat(context).hasNotFailed().doesNotHaveBean(WorkflowConsumerLifecycle.class));
    }

    @Test void overlappingMembershipBindsActualPublishingChildBeforeApplicationDispatch() {
        var config = new ConsumerConfiguration();
        var scheduler = mock(ScheduledExecutorService.class);
        when(scheduler.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.SECONDS)))
                .thenReturn(mock(ScheduledFuture.class));
        var downstream = mock(ApplicationEventPublisher.class);
        var meters = new SimpleMeterRegistry();
        try (var failures = new PartitionFailureHandler(new ConsumerMetrics(meters), scheduler)) {
            var factory = config.workflowKafkaListenerContainerFactory(
                    config.workflowConsumerFactory(new KafkaProperties()), failures, downstream);
            var parent = spy(factory.createContainer(ConsumerConfiguration.TOPIC));
            var oldChild = mock(MessageListenerContainer.class);
            var newChild = mock(MessageListenerContainer.class);
            var oldConsumer = mock(Consumer.class);
            var newConsumer = mock(Consumer.class);
            var p0 = new TopicPartition(ConsumerConfiguration.TOPIC, 0);
            var p1 = new TopicPartition(ConsumerConfiguration.TOPIC, 1);
            when(oldChild.getAssignedPartitions()).thenReturn(List.of(p0));
            when(newChild.getAssignedPartitions()).thenReturn(List.of(p0, p1));
            doReturn(oldChild).when(parent).getContainerFor(p0.topic(), 0);
            var publisher = (ApplicationEventPublisher) ReflectionTestUtils.getField(parent, "applicationEventPublisher");
            var rebalance = (ConsumerAwareRebalanceListener) parent.getContainerProperties().getConsumerRebalanceListener();
            // Application dispatch may be asynchronous; binding must already exist when it is invoked.
            doAnswer(call -> {
                Object event = call.getArgument(0);
                if (event instanceof ConsumerStartingEvent start) {
                    var owner = start.getSource(MessageListenerContainer.class) == oldChild ? oldConsumer : newConsumer;
                    rebalance.onPartitionsAssigned(owner, owner == oldConsumer ? List.of(p0) : List.of(p0, p1));
                }
                return null;
            }).when(downstream).publishEvent(any(Object.class));
            publisher.publishEvent(new ConsumerStartingEvent(oldChild, parent));
            publisher.publishEvent(new ConsumerStartingEvent(newChild, parent));
            assertThat(oldChild.getAssignedPartitions()).contains(p0);
            assertThat(newChild.getAssignedPartitions()).contains(p0);
            clearInvocations(oldChild, newChild);
            var failed = new ConsumerRecord<>(p0.topic(), 0, 4, "key", "body");
            failures.handleOne(new CommitFailedException(), failed, newConsumer, parent);
            verify(newChild).pausePartition(p0);
            verify(oldChild, never()).pausePartition(any());
            verify(parent, never()).getContainerFor(anyString(), anyInt());
            var healthy = new ConsumerRecord<>(p1.topic(), 1, 8, "key", "body");
            failures.succeeded(healthy, ProcessingOutcome.APPLIED, failures.processingAssignment(healthy));
            assertThat(meters.get("consumer.applied").tag("partition", "1").counter().count()).isEqualTo(1);
            verify(newChild, never()).pausePartition(p1);
            verify(newChild, never()).pause();
            rebalance.onPartitionsLost(oldConsumer, List.of(p0));
            var task = ArgumentCaptor.forClass(Runnable.class);
            verify(scheduler).schedule(task.capture(), eq(1L), eq(TimeUnit.SECONDS));
            task.getValue().run();
            verify(newChild).resumePartition(p0);
            verify(oldChild, never()).resumePartition(any());
            assertThat(failures.processingAssignment(failed)).isNotNull();
            publisher.publishEvent(new ConsumerStoppedEvent(newChild, parent, ConsumerStoppedEvent.Reason.NORMAL));
            assertThat(failures.processingAssignment(failed)).isNull();
            verify(downstream, times(3)).publishEvent(any(Object.class));
        }
    }

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
            var factory = config.workflowKafkaListenerContainerFactory(consumer, failures, event -> {});
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
