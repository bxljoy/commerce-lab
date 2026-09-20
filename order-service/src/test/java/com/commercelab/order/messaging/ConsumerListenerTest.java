package com.commercelab.order.messaging;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.commercelab.order.events.EventJson;
import com.commercelab.order.events.EventProtocolException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.MDC;

class ConsumerListenerTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void invocationEntryEpochFencesLateSuccessAfterHandover(boolean transientFailure) throws Exception {
        var application = mock(OrderResultHandler.class);
        var scheduler = mock(ScheduledExecutorService.class);
        var timer = mock(ScheduledFuture.class);
        when(scheduler.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.SECONDS))).thenReturn(timer);
        var meters = new SimpleMeterRegistry();
        var oldConsumer = mock(Consumer.class);
        var newConsumer = mock(Consumer.class);
        var oldChild = mock(MessageListenerContainer.class);
        var newChild = mock(MessageListenerContainer.class);
        var p0 = new TopicPartition(ConsumerConfiguration.TOPIC, 0);
        var worker = Executors.newSingleThreadExecutor();
        try (var failures = new PartitionFailureHandler(new ConsumerMetrics(meters), scheduler);
                var input = getClass().getResourceAsStream("/contracts/events/inventory/v1/inventory-reserved.json")) {
            var events = new EventJson();
            String body = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            String key = events.content(body).path("orderId").asText();
            var record = record(p0.topic(), 0, 4, key, body);
            var listener = new InventoryResultListener(application, events, (id, outcome) -> {}, failures);
            failures.consumerLifecycle(new ConsumerStartingEvent(oldChild, oldChild));
            failures.rebalanceListener().onPartitionsAssigned(oldConsumer, List.of(p0));
            Exception failure = transientFailure ? new CommitFailedException() : new EventProtocolException("INVALID_JSON");
            when(application.handle(key, body)).thenAnswer(call -> {
                worker.submit(() -> {
                    failures.consumerLifecycle(new ConsumerStartingEvent(newChild, oldChild));
                    failures.rebalanceListener().onPartitionsAssigned(newConsumer, List.of(p0));
                    failures.handleOne(failure, record, newConsumer, newChild);
                }).get(5, TimeUnit.SECONDS);
                return ProcessingOutcome.APPLIED;
            });
            listener.onRecord(record);
            assertThat(meters.get("consumer.blocked.partitions").tag("partition", "0").gauge().value()).isEqualTo(1);
            assertThat(failures.processingAssignment(record)).isNull(); // Old thread cannot borrow B's epoch.
            verify(timer, never()).cancel(anyBoolean());
            if (transientFailure) {
                var task = ArgumentCaptor.forClass(Runnable.class);
                verify(scheduler).schedule(task.capture(), eq(1L), eq(TimeUnit.SECONDS));
                task.getValue().run();
                verify(newChild).resumePartition(p0);
            } else {
                verifyNoInteractions(scheduler);
                verify(newChild, never()).resumePartition(any());
            }
            failures.handleOne(failure, record(p0.topic(), 0, 5, key, body), newConsumer, newChild);
            verify(newConsumer, times(2)).seek(p0, 4);
            verify(newConsumer, never()).seek(p0, 5);
        } finally {
            worker.shutdownNow();
            assertThat(worker.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"success", "handler", "hook"})
    void restoresMdcOnEveryExitAndHookFollowsHandler(String failure) throws Exception {
        var handler = mock(OrderResultHandler.class);
        var hook = mock(ConsumerCommitHook.class);
        var events = new EventJson();
        try (var failures = new PartitionFailureHandler(new ConsumerMetrics(new SimpleMeterRegistry()));
                var input = getClass().getResourceAsStream("/contracts/events/inventory/v1/inventory-reserved.json")) {
            String body = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            var json = events.content(body);
            String key = json.path("orderId").asText();
            UUID event = UUID.fromString(json.path("eventId").asText());
            var record = record(ConsumerConfiguration.TOPIC, 0, 4, key, body);
            var listener = new InventoryResultListener(handler, events, hook, failures);
            when(handler.handle(key, body)).thenAnswer(call -> {
                assertThat(MDC.get("eventId")).isEqualTo(event.toString());
                assertThat(MDC.get("orderId")).isEqualTo(key);
                assertThat(MDC.get("unrelated")).isNull();
                if (failure.equals("handler")) throw new IllegalStateException("private-error");
                return ProcessingOutcome.APPLIED;
            });
            if (failure.equals("hook")) doThrow(new IllegalStateException("private-error"))
                    .when(hook).afterDatabaseCommit(event, ProcessingOutcome.APPLIED);
            MDC.put("unrelated", "prior");
            try {
                if (failure.equals("success")) listener.onRecord(record);
                else assertThatThrownBy(() -> listener.onRecord(record)).isInstanceOf(IllegalStateException.class);
                assertThat(MDC.getCopyOfContextMap()).containsExactlyEntriesOf(Map.of("unrelated", "prior"));
                var order = inOrder(handler, hook);
                order.verify(handler).handle(key, body);
                if (failure.equals("handler")) verifyNoInteractions(hook);
                else order.verify(hook).afterDatabaseCommit(event, ProcessingOutcome.APPLIED);
            } finally { MDC.clear(); }
        }
    }

    @Test void malformedInputNeverReachesTransactionOrHookAndLeavesNoMdc() {
        var handler = mock(OrderResultHandler.class);
        var hook = mock(ConsumerCommitHook.class);
        try (var failures = new PartitionFailureHandler(new ConsumerMetrics(new SimpleMeterRegistry()))) {
            var listener = new InventoryResultListener(handler, new EventJson(), hook, failures);
            assertThatThrownBy(() -> listener.onRecord(record(ConsumerConfiguration.TOPIC, 0, 4,
                    "private-key", "{sensitive-input"))).isInstanceOf(EventProtocolException.class);
            verifyNoInteractions(handler, hook);
            assertThat(MDC.getCopyOfContextMap()).isNullOrEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"key", "value"})
    void malformedUtf8NeverReachesTransactionOrLeaksPayload(String field) throws Exception {
        var handler = mock(OrderResultHandler.class);
        var hook = mock(ConsumerCommitHook.class);
        try (var failures = new PartitionFailureHandler(new ConsumerMetrics(new SimpleMeterRegistry()));
                var input = getClass().getResourceAsStream(
                        "/contracts/events/inventory/v1/inventory-reserved.json")) {
            var events = new EventJson();
            String body = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            String key = events.content(body).path("orderId").asText();
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = body.getBytes(StandardCharsets.UTF_8);
            byte[] target = field.equals("key") ? keyBytes : valueBytes;
            target[target.length / 2] = (byte) 0xff;
            var listener = new InventoryResultListener(handler, events, hook, failures);

            var failure = catchThrowableOfType(() -> listener.onRecord(new ConsumerRecord<>(
                    ConsumerConfiguration.TOPIC, 0, 4, keyBytes, valueBytes)), EventProtocolException.class);

            assertThat(failure.code()).isEqualTo("INVALID_UTF8");
            assertThat(failure).hasMessage("INVALID_UTF8").hasNoCause();
            verifyNoInteractions(handler, hook);
            assertThat(MDC.getCopyOfContextMap()).isNullOrEmpty();
        }
    }

    private static ConsumerRecord<byte[], byte[]> record(
            String topic, int partition, long offset, String key, String value) {
        return new ConsumerRecord<>(topic, partition, offset,
                key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }
}
