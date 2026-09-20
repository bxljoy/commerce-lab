package com.commercelab.order.persistence;

import com.commercelab.order.events.EventProtocolException;
import com.commercelab.order.events.InventoryResult;
import com.commercelab.order.outbox.OrderOutboxStore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
@Transactional(propagation = Propagation.MANDATORY)
public class OrderCompletionStore {
    private final JdbcTemplate jdbc;
    private final OrderOutboxStore outbox;
    private final ObjectMapper mapper;

    public OrderCompletionStore(JdbcTemplate jdbc, OrderOutboxStore outbox, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.mapper = mapper;
    }

    public void complete(InventoryResult result) {
        String current = jdbc.query("SELECT status FROM orders WHERE id = ? FOR UPDATE",
                rs -> rs.next() ? rs.getString(1) : null, result.orderId());
        if (current == null) throw new EventProtocolException("UNKNOWN_ORDER");
        String payload = outbox.findOrderPlacedPayload(result.orderId())
                .orElseThrow(() -> new EventProtocolException("MISSING_ORDER_INTENT"));
        validateOriginal(payload, result);
        if (!current.equals("PENDING_INVENTORY")) throw new EventProtocolException("ORDER_ALREADY_TERMINAL");

        boolean rejected = switch (result.eventType()) {
            case "InventoryReserved" -> false;
            case "InventoryRejected" -> {
                if (!"INSUFFICIENT_STOCK".equals(result.reasonCode()))
                    throw new EventProtocolException("INVALID_REASON_CODE");
                yield true;
            }
            default -> throw new EventProtocolException("UNSUPPORTED_EVENT_TYPE");
        };
        jdbc.update("""
                INSERT INTO order_inventory_results(order_id, causation_id, result_event_id, result_type)
                VALUES (?, ?, ?, ?)
                """, result.orderId(), result.causationId(), result.eventId(), result.eventType());
        int updated = jdbc.update("""
                UPDATE orders SET status=?, rejection_reason=?, version=version+1,
                  next_attempt_at=NULL, recovery_blocked=FALSE, last_failure_code=NULL
                WHERE id=? AND status='PENDING_INVENTORY'
                """, rejected ? "REJECTED" : "CONFIRMED", rejected ? "STOCK_UNAVAILABLE" : null, result.orderId());
        if (updated != 1) throw new EventProtocolException("ORDER_TRANSITION_CONFLICT");
    }

    private void validateOriginal(String payload, InventoryResult result) {
        JsonNode original;
        try {
            original = mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(payload);
        } catch (JsonProcessingException e) {
            throw new EventProtocolException("INVALID_ORDER_INTENT");
        }
        if (original == null || !original.isObject()
                || !textEquals(original, "orderId", result.orderId().toString())
                || !textEquals(original, "eventId", result.causationId().toString())
                || !textEquals(original, "correlationId", result.correlationId())
                || !mapper.valueToTree(result.lines()).equals(original.get("lines")))
            throw new EventProtocolException("ORDER_INTENT_MISMATCH");
    }

    private boolean textEquals(JsonNode node, String field, String expected) {
        return node.path(field).isTextual() && expected.equals(node.path(field).textValue());
    }
}
