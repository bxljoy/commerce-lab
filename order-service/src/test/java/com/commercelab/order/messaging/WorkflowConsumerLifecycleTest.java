package com.commercelab.order.messaging;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

class WorkflowConsumerLifecycleTest {
    final KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
    final MessageListenerContainer container = mock(MessageListenerContainer.class);
    final ScheduledExecutorService worker = mock(ScheduledExecutorService.class);
    final ScheduledFuture<?> timer = mock(ScheduledFuture.class);

    WorkflowConsumerLifecycle lifecycle() {
        when(registry.getListenerContainer("order-workflow")).thenReturn(container);
        doReturn(timer).when(worker).scheduleWithFixedDelay(any(Runnable.class), eq(0L), eq(5L), eq(TimeUnit.SECONDS));
        return new WorkflowConsumerLifecycle(registry, "order-workflow", worker);
    }

    Runnable attempt() {
        var task = ArgumentCaptor.forClass(Runnable.class);
        verify(worker).scheduleWithFixedDelay(task.capture(), eq(0L), eq(5L), eq(TimeUnit.SECONDS));
        return task.getValue();
    }


    @Test void realConcurrentContainerCleansFirstChildWhenSecondConstructionFails() throws Exception {
        var consumers = new java.util.concurrent.CopyOnWriteArrayList<org.apache.kafka.clients.consumer.MockConsumer<String, String>>();
        var created = new java.util.concurrent.atomic.AtomicInteger();
        var factory = new org.springframework.kafka.core.DefaultKafkaConsumerFactory<String, String>(
                java.util.Map.of("group.id", "startup-test", "enable.auto.commit", false)) {
            @Override public org.apache.kafka.clients.consumer.Consumer<String, String> createConsumer(
                    String group, String prefix, String suffix, java.util.Properties properties) {
                if (created.incrementAndGet() == 2) throw new org.apache.kafka.common.config.ConfigException("DNS");
                var consumer = new org.apache.kafka.clients.consumer.MockConsumer<String, String>(
                        org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST);
                consumers.add(consumer);
                return consumer;
            }
        };
        var properties = new org.springframework.kafka.listener.ContainerProperties("startup-topic");
        properties.setMessageListener((org.springframework.kafka.listener.MessageListener<String, String>) record -> {});
        properties.setConsumerStartTimeout(java.time.Duration.ofSeconds(2));
        var parent = new org.springframework.kafka.listener.ConcurrentMessageListenerContainer<>(factory, properties);
        parent.setConcurrency(3);
        when(registry.getListenerContainer("order-workflow")).thenReturn(parent);
        doReturn(timer).when(worker).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
        try (var lifecycle = new WorkflowConsumerLifecycle(registry, "order-workflow", worker)) {
            lifecycle.start();
            var retry = attempt();
            retry.run();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(3))
                    .until(() -> consumers.getFirst().closed());
            assertThat(parent.isRunning()).isFalse();
            assertThat(parent.getContainers()).isEmpty();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(3)).untilAsserted(() -> {
                retry.run();
                assertThat(parent.getContainers()).hasSize(3);
            });
            assertThat(created.get()).isEqualTo(5);
        } finally { parent.stop(); }
        assertThat(consumers).allMatch(org.apache.kafka.clients.consumer.MockConsumer::closed);
    }

    @Test void shutdownDuringInFlightStartStopsLateContainerAndTerminatesWorker() throws Exception {
        var executor = Executors.newSingleThreadScheduledExecutor();
        var entering = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var stopped = new CountDownLatch(1);
        when(registry.getListenerContainer("order-workflow")).thenReturn(container);
        doAnswer(call -> { entering.countDown(); release.await(3, TimeUnit.SECONDS); return null; })
                .when(container).start();
        doAnswer(call -> { ((Runnable) call.getArgument(0)).run(); stopped.countDown(); return null; })
                .when(container).stop(any(Runnable.class));
        var lifecycle = new WorkflowConsumerLifecycle(registry, "order-workflow", executor);
        try {
            lifecycle.start();
            assertThat(entering.await(3, TimeUnit.SECONDS)).isTrue();
            lifecycle.close();
            assertThat(lifecycle.isRunning()).isFalse();
            assertThat(stopped.getCount()).isEqualTo(1);
            release.countDown();
            assertThat(stopped.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(executor.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            verify(container).start();
        } finally { release.countDown(); lifecycle.close(); executor.shutdownNow(); }
    }

    @Test void startupIsDeferredAndApplicationThreadNeverConstructsConsumer() {
        try (var lifecycle = lifecycle()) {
            assertThat(lifecycle.isAutoStartup()).isFalse();
            assertThat(lifecycle.getPhase()).isGreaterThan(registry.getPhase());
            lifecycle.onReady();
            lifecycle.onReady();
            verify(container, never()).start();
            attempt().run();
            attempt().run();
            verify(container).start();
        }
    }

    @Test void failedPartialStartWaitsForStopCallbackBeforeRetry() {
        try (var lifecycle = lifecycle()) {
            var stopped = new AtomicReference<Runnable>();
            doThrow(new org.apache.kafka.common.config.ConfigException("No resolvable bootstrap urls"))
                    .doNothing().when(container).start();
            doAnswer(call -> { stopped.set(call.getArgument(0)); return null; }).when(container).stop(any(Runnable.class));
            lifecycle.start();
            var retry = attempt();
            assertThatCode(retry::run).doesNotThrowAnyException();
            assertThat(lifecycle.isRunning()).isTrue();
            verify(container).stop(any(Runnable.class));
            retry.run();
            verify(container).start();
            stopped.get().run();
            retry.run();
            retry.run();
            verify(container, times(2)).start();
        }
    }

    @Test void cleanupFailureDoesNotPermitOverlappingConsumers() {
        try (var lifecycle = lifecycle()) {
            doThrow(new IllegalStateException()).when(container).start();
            doThrow(new IllegalStateException()).when(container).stop(any(Runnable.class));
            lifecycle.start();
            var retry = attempt();
            assertThatCode(retry::run).doesNotThrowAnyException();
            retry.run();
            verify(container).start();
        }
    }

    @Test void stopCancelsQueuedRetriesAndCallbackWaitsForConsumerCleanup() {
        var lifecycle = lifecycle();
        var stopped = new AtomicReference<Runnable>();
        doAnswer(call -> { stopped.set(call.getArgument(0)); return null; }).when(container).stop(any(Runnable.class));
        lifecycle.start();
        var retry = attempt();
        retry.run();
        var callback = mock(Runnable.class);
        lifecycle.stop(callback);
        assertThat(lifecycle.isRunning()).isFalse();
        verify(timer).cancel(false);
        retry.run();
        verify(container).start();
        var cleanup = ArgumentCaptor.forClass(Runnable.class);
        verify(worker).execute(cleanup.capture());
        cleanup.getValue().run();
        verify(callback, never()).run();
        stopped.get().run();
        verify(callback).run();
        lifecycle.close();
        verify(worker).shutdown();
        lifecycle.start();
        verify(worker).scheduleWithFixedDelay(any(), eq(0L), eq(5L), eq(TimeUnit.SECONDS));
    }

    @Test void stopDuringFailedStartReusesPendingPartialCleanup() {
        try (var lifecycle = lifecycle()) {
            doThrow(new IllegalStateException()).when(container).start();
            var stopped = new AtomicReference<Runnable>();
            doAnswer(call -> { stopped.set(call.getArgument(0)); return null; }).when(container).stop(any(Runnable.class));
            lifecycle.start();
            attempt().run();
            var callback = mock(Runnable.class);
            lifecycle.stop(callback);
            var cleanup = ArgumentCaptor.forClass(Runnable.class);
            verify(worker).execute(cleanup.capture());
            cleanup.getValue().run();
            verify(container).stop(any(Runnable.class));
            verify(callback, never()).run();
            stopped.get().run();
            verify(callback).run();
        }
    }
}
