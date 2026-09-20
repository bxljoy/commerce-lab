package com.commercelab.order.messaging;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.commercelab.order.AbstractPostgresIntegrationTest;
import com.commercelab.order.events.EventProtocolException;
import com.commercelab.order.service.OrderService;
import com.commercelab.order.service.PlaceOrderCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest
@AutoConfigureMockMvc
class OrderResultHandlerIT extends AbstractPostgresIntegrationTest {
    @Autowired OrderResultHandler handler;
    @Autowired OrderService orders;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired MockMvc mvc;
    @Autowired PlatformTransactionManager transactions;

    @BeforeEach void clear() {
        jdbc.execute("TRUNCATE order_outbox, order_requests, order_lines, orders CASCADE");
        jdbc.execute("TRUNCATE order_event_inbox CASCADE");
    }

    @ParameterizedTest
    @ValueSource(strings = {"InventoryReserved", "InventoryRejected"})
    void completionIsAtomicVersionedAndVisibleThroughGetAndPostReplay(String type) throws Exception {
        UUID id = pending();
        long version = version(id);
        jdbc.update("UPDATE orders SET next_attempt_at=now(), recovery_blocked=TRUE, last_failure_code='OLD' WHERE id=?", id);
        String result = resultFor(id, type);
        assertThat(handler.handle(id.toString(), result)).isEqualTo(ProcessingOutcome.APPLIED);
        assertThat(handler.handle(id.toString(), result)).isEqualTo(ProcessingOutcome.DUPLICATE);
        assertThat(handler.handle(id.toString(), reordered(result))).isEqualTo(ProcessingOutcome.DUPLICATE);
        String expected = type.equals("InventoryReserved") ? "CONFIRMED" : "REJECTED";
        String reason = type.equals("InventoryReserved") ? null : "STOCK_UNAVAILABLE";
        assertThat(orders.getOrder(id).status().name()).isEqualTo(expected);
        assertThat(version(id)).isEqualTo(version + 1);
        var row = jdbc.queryForMap("SELECT * FROM orders WHERE id=?", id);
        assertThat(row.get("rejection_reason")).isEqualTo(reason);
        assertThat(row.get("next_attempt_at")).isNull();
        assertThat(row.get("recovery_blocked")).isEqualTo(false);
        assertThat(row.get("last_failure_code")).isNull();
        assertCounts(1);
        var accepted = jdbc.queryForMap("SELECT * FROM order_inventory_results WHERE order_id=?", id);
        var event = mapper.readTree(result);
        assertThat(accepted.get("causation_id")).isEqualTo(UUID.fromString(event.path("causationId").asText()));
        assertThat(accepted.get("result_event_id")).isEqualTo(UUID.fromString(event.path("eventId").asText()));
        assertThat(accepted.get("result_type")).isEqualTo(type);
        mvc.perform(get("/api/v1/orders/{id}", id)).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(expected)).andExpect(jsonPath("$.rejectionReason").value(reason));
        mvc.perform(post("/api/v1/orders").header("Idempotency-Key", "request")
                .header("X-Correlation-ID", "replay").contentType("application/json").content("""
                {"customerId":"cust","currency":"EUR","lines":[
                 {"sku":"B","quantity":2,"unitPrice":1},{"sku":"A","quantity":1,"unitPrice":1}]}
                """))
                .andExpect(status().isOk()).andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.status").value(expected)).andExpect(jsonPath("$.rejectionReason").value(reason));
    }

    @ParameterizedTest
    @ValueSource(strings = {"causationId", "correlationId", "orderId", "sku", "quantity", "lineOrder", "key", "shortage"})
    void mismatchesCannotClaimInboxOrChangeOrder(String field) throws Exception {
        UUID id = pending();
        ObjectNode body = (ObjectNode) mapper.readTree(resultFor(id, "InventoryReserved"));
        String key = id.toString();
        switch (field) {
            case "causationId" -> body.put(field, UUID.randomUUID().toString());
            case "correlationId" -> body.put(field, "other");
            case "orderId" -> {
                UUID other = orders.placeOrder("other", command(), "origin").order().id();
                body.put(field, other.toString());
                key = other.toString();
            }
            case "sku" -> ((ObjectNode) body.path("lines").get(0)).put("sku", "C");
            case "quantity" -> ((ObjectNode) body.path("lines").get(0)).put("quantity", 3);
            case "lineOrder" -> {
                ArrayNode lines = (ArrayNode) body.path("lines");
                var first = lines.remove(0);
                lines.add(first);
            }
            case "key" -> key = UUID.randomUUID().toString();
            case "shortage" -> {
                body = (ObjectNode) mapper.readTree(resultFor(id, "InventoryRejected"));
                ((ObjectNode) body.path("shortages").get(0)).put("available", 2);
            }
            default -> throw new AssertionError(field);
        }
        String finalKey = key;
        String value = body.toString();
        assertThatThrownBy(() -> handler.handle(finalKey, value)).isInstanceOf(EventProtocolException.class);
        assertPending(id);
        assertCounts(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "null", "blank", "invalid", "different"})
    void invalidOriginalCorrelationRollsBack(String mutation) throws Exception {
        UUID id = pending();
        String result = resultFor(id, "InventoryReserved");
        ObjectNode original = original(id);
        switch (mutation) {
            case "missing" -> original.remove("correlationId");
            case "null" -> original.putNull("correlationId");
            case "blank" -> original.put("correlationId", "");
            case "invalid" -> original.put("correlationId", "bad correlation");
            case "different" -> original.put("correlationId", "different");
        }
        jdbc.update("UPDATE order_outbox SET payload=? WHERE order_id=?", original.toString(), id);
        assertThatThrownBy(() -> handler.handle(id.toString(), result)).isInstanceOf(EventProtocolException.class);
        assertPending(id);
        assertCounts(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"unknown", "missingIntent", "historical", "terminal"})
    void unknownAndUnenrolledAndTerminalOrdersAreNotCompleted(String kind) throws Exception {
        UUID id = pending();
        ObjectNode result = (ObjectNode) mapper.readTree(resultFor(id, "InventoryReserved"));
        if (kind.equals("unknown")) result.put("orderId", UUID.randomUUID().toString());
        if (kind.equals("missingIntent") || kind.equals("historical"))
            jdbc.update("DELETE FROM order_outbox WHERE order_id=?", id);
        if (kind.equals("historical") || kind.equals("terminal"))
            jdbc.update("UPDATE orders SET status='CONFIRMED' WHERE id=?", id);
        var before = jdbc.queryForMap("SELECT * FROM orders WHERE id=?", id);
        assertThatThrownBy(() -> handler.handle(result.path("orderId").asText(), result.toString()))
                .isInstanceOf(EventProtocolException.class);
        assertThat(jdbc.queryForMap("SELECT * FROM orders WHERE id=?", id)).isEqualTo(before);
        assertCounts(0);
    }

    @Test void changedContentAndDifferentResultIdentityCannotReplaceAcceptedOutcome() throws Exception {
        UUID id = pending();
        String reserved = resultFor(id, "InventoryReserved");
        handler.handle(id.toString(), reserved);
        var before = jdbc.queryForMap("SELECT * FROM orders WHERE id=?", id);
        ObjectNode changed = (ObjectNode) mapper.readTree(reserved);
        changed.put("occurredAt", "2026-09-21T00:00:00Z");
        assertThatThrownBy(() -> handler.handle(id.toString(), changed.toString()))
                .isInstanceOf(EventProtocolException.class).hasMessage("EVENT_IDENTITY_CONFLICT");
        for (String type : List.of("InventoryReserved", "InventoryRejected")) {
            String other = resultFor(id, type);
            assertThatThrownBy(() -> handler.handle(id.toString(), other)).isInstanceOf(EventProtocolException.class);
        }
        assertThat(jdbc.queryForMap("SELECT * FROM orders WHERE id=?", id)).isEqualTo(before);
        assertCounts(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void concurrentDeliveryHasOneWinner(boolean conflicting) throws Exception {
        UUID id = pending();
        String first = resultFor(id, "InventoryReserved");
        String second = conflicting ? resultFor(id, "InventoryRejected") : first;
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> race(start, id, first));
            var b = executor.submit(() -> race(start, id, second));
            start.countDown();
            assertThat(List.of(a.get(15, TimeUnit.SECONDS), b.get(15, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder("APPLIED", conflicting ? "CONFLICT" : "DUPLICATE");
        }
        assertCounts(1);
        assertThat(version(id)).isEqualTo(1);
        String type = jdbc.queryForObject("SELECT result_type FROM order_inventory_results", String.class);
        assertThat(orders.getOrder(id).status().name())
                .isEqualTo(type.equals("InventoryReserved") ? "CONFIRMED" : "REJECTED");
    }

    @Test void rollbackAfterCompletionRemovesInboxAcceptanceAndStatusAndAllowsRetry() throws Exception {
        UUID id = pending();
        String result = resultFor(id, "InventoryReserved");
        assertThatThrownBy(() -> new TransactionTemplate(transactions).executeWithoutResult(tx -> {
            handler.handle(id.toString(), result);
            assertCounts(1);
            throw new IllegalStateException("forced rollback");
        })).isInstanceOf(IllegalStateException.class).hasMessage("forced rollback");
        assertPending(id);
        assertCounts(0);
        assertThat(handler.handle(id.toString(), result)).isEqualTo(ProcessingOutcome.APPLIED);
    }

    @Test void databaseFailureAfterAcceptanceRollsBackInboxAndAcceptedIdentity() throws Exception {
        UUID id = pending();
        String result = resultFor(id, "InventoryReserved");
        jdbc.execute("ALTER TABLE orders ADD CONSTRAINT task4_force_pending CHECK (status='PENDING_INVENTORY')");
        try {
            assertThatThrownBy(() -> handler.handle(id.toString(), result))
                    .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
            assertPending(id);
            assertCounts(0);
        } finally {
            jdbc.execute("ALTER TABLE orders DROP CONSTRAINT task4_force_pending");
        }
        assertThat(handler.handle(id.toString(), result)).isEqualTo(ProcessingOutcome.APPLIED);
    }

    private String race(CountDownLatch start, UUID id, String value) throws Exception {
        assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
        try { return handler.handle(id.toString(), value).name(); }
        catch (EventProtocolException e) { return "CONFLICT"; }
    }

    private UUID pending() { return orders.placeOrder("request", command(), "origin").order().id(); }

    private PlaceOrderCommand command() {
        return new PlaceOrderCommand("cust", "EUR", List.of(new PlaceOrderCommand.Line("B", 2, BigDecimal.ONE),
                new PlaceOrderCommand.Line("A", 1, BigDecimal.ONE)));
    }

    private ObjectNode original(UUID id) throws Exception {
        return (ObjectNode) mapper.readTree(jdbc.queryForObject(
                "SELECT payload FROM order_outbox WHERE order_id=?", String.class, id));
    }

    private String resultFor(UUID id, String type) throws Exception {
        ObjectNode body = original(id);
        body.put("causationId", body.path("eventId").asText());
        body.put("eventId", UUID.randomUUID().toString());
        body.put("eventType", type);
        if (type.equals("InventoryRejected")) {
            body.put("reasonCode", "INSUFFICIENT_STOCK");
            body.putArray("shortages").addObject().put("sku", "B").put("requested", 2).put("available", 0);
        }
        return body.toString();
    }

    private String reordered(String value) throws Exception {
        var body = mapper.readTree(value);
        List<String> fields = new ArrayList<>();
        body.fieldNames().forEachRemaining(fields::add);
        Collections.reverse(fields);
        ObjectNode reordered = mapper.createObjectNode();
        fields.forEach(field -> reordered.set(field, body.get(field)));
        return reordered.toPrettyString();
    }

    private long version(UUID id) { return jdbc.queryForObject("SELECT version FROM orders WHERE id=?", Long.class, id); }

    private void assertPending(UUID id) {
        assertThat(orders.getOrder(id).status().name()).isEqualTo("PENDING_INVENTORY");
        assertThat(version(id)).isZero();
    }

    private void assertCounts(long expected) {
        for (String table : List.of("order_event_inbox", "order_inventory_results"))
            assertThat(jdbc.queryForObject("SELECT count(*) FROM " + table, Long.class)).as(table).isEqualTo(expected);
    }
}
