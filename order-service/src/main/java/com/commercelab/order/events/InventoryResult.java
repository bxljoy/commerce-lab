package com.commercelab.order.events;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record InventoryResult(UUID eventId, String eventType, int schemaVersion,
        Instant occurredAt, UUID orderId, String correlationId, UUID causationId,
        List<Line> lines, String reasonCode, List<Shortage> shortages) {
    public InventoryResult {
        lines = List.copyOf(lines);
        shortages = shortages == null ? null : List.copyOf(shortages);
    }

    public record Line(String sku, int quantity) {}
    public record Shortage(String sku, int requested, int available) {}
}
