package com.commercelab.order.outbox;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Accepted and persisted order intent; not evidence of inventory reservation. */
public record OrderPlacedEvent(UUID eventId, String eventType, int schemaVersion, Instant occurredAt,
        UUID orderId, String correlationId, List<Line> lines) {
    public OrderPlacedEvent {
        lines = List.copyOf(lines);
    }

    public record Line(String sku, int quantity) {}
}
