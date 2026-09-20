package com.commercelab.inventory.outbox;

import com.commercelab.inventory.messaging.InventoryEventHandler;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.springframework.jdbc.core.JdbcTemplate;

final class InventoryOutboxTestSupport {
    private InventoryOutboxTestSupport() {}

    static OutboxMessage seed(InventoryEventHandler handler, JdbcTemplate jdbc, boolean rejected, boolean large) {
        UUID order = UUID.randomUUID();
        String lines = IntStream.range(0, large ? 40 : 1).mapToObj(i -> {
            String sku = "relay-" + i + (large ? "x".repeat(50) : "");
            jdbc.update("INSERT INTO stock(sku, available_quantity) VALUES (?, 100) ON CONFLICT (sku) DO NOTHING", sku);
            return "{\"sku\":\"" + sku + "\",\"quantity\":" + (rejected ? 101 : 1) + "}";
        }).collect(Collectors.joining(","));
        String body = """
                {"eventId":"%s","eventType":"OrderPlaced","schemaVersion":1,
                 "occurredAt":"2026-09-20T00:00:00Z","orderId":"%s",
                 "correlationId":"inventory-relay-it","lines":[%s]}
                """.formatted(UUID.randomUUID(), order, lines);
        handler.handle(order.toString(), body);
        return jdbc.queryForObject("SELECT * FROM inventory_result_outbox WHERE order_id=?",
                (rs, n) -> new OutboxMessage(rs.getObject("event_id", UUID.class), order,
                        rs.getString("topic"), rs.getString("message_key"), rs.getString("payload")), order);
    }
}
