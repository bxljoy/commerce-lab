package com.commercelab.inventory.messaging;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.commercelab.inventory.events.EventJson;
import com.commercelab.inventory.events.EventProtocolException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.MDC;

class ConsumerListenerTest {
    @ParameterizedTest
    @ValueSource(strings = {"success", "handler", "hook"})
    void restoresMdcOnEveryExitAndHookFollowsHandler(String failure) throws Exception {
        var handler = mock(InventoryEventHandler.class);
        var hook = mock(ConsumerCommitHook.class);
        var events = new EventJson();
        try (var failures = new PartitionFailureHandler(new ConsumerMetrics(new SimpleMeterRegistry()));
                var input = getClass().getResourceAsStream("/contracts/events/orders/v1/order-placed.json")) {
            String body = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            var json = events.content(body);
            String key = json.path("orderId").asText();
            UUID event = UUID.fromString(json.path("eventId").asText());
            var record = new ConsumerRecord<>(ConsumerConfiguration.TOPIC, 0, 4, key, body);
            var listener = new OrderPlacedListener(handler, events, hook, failures);
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
        var handler = mock(InventoryEventHandler.class);
        var hook = mock(ConsumerCommitHook.class);
        try (var failures = new PartitionFailureHandler(new ConsumerMetrics(new SimpleMeterRegistry()))) {
            var listener = new OrderPlacedListener(handler, new EventJson(), hook, failures);
            assertThatThrownBy(() -> listener.onRecord(new ConsumerRecord<>(ConsumerConfiguration.TOPIC, 0, 4,
                    "private-key", "{sensitive-input"))).isInstanceOf(EventProtocolException.class);
            verifyNoInteractions(handler, hook);
            assertThat(MDC.getCopyOfContextMap()).isNullOrEmpty();
        }
    }
}
