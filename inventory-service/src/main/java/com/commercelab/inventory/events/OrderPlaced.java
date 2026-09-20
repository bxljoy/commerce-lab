package com.commercelab.inventory.events;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderPlaced(UUID eventId, String eventType, int schemaVersion,
        Instant occurredAt, UUID orderId, String correlationId, List<Line> lines) {
    public OrderPlaced {
        lines = List.copyOf(lines);
    }

    public record Line(String sku, int quantity) {}
}
