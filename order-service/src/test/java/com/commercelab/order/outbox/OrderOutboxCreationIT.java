package com.commercelab.order.outbox;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.commercelab.order.AbstractPostgresIntegrationTest;
import com.commercelab.order.service.OrderCreationService;
import com.commercelab.order.service.PlaceOrderCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@SpringBootTest
@AutoConfigureMockMvc
class OrderOutboxCreationIT extends AbstractPostgresIntegrationTest {
    static final AtomicInteger INVENTORY_CALLS = new AtomicInteger();
    static final HttpServer INVENTORY;
    static {
        try {
            INVENTORY = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            INVENTORY.createContext("/", exchange -> {
                INVENTORY_CALLS.incrementAndGet();
                exchange.sendResponseHeaders(503, -1);
                exchange.close();
            });
            INVENTORY.start();
        } catch (java.io.IOException ex) { throw new ExceptionInInitializerError(ex); }
    }
    @DynamicPropertySource static void inventory(DynamicPropertyRegistry registry) {
        registry.add("inventory.base-url", () -> "http://127.0.0.1:" + INVENTORY.getAddress().getPort());
    }
    @AfterAll static void stop() { INVENTORY.stop(0); }
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @Autowired OrderCreationService creation;
    @Autowired ApplicationContext context;
    @SpyBean OrderOutboxStore outbox;

    @BeforeEach void clear() {
        jdbc.execute("TRUNCATE order_outbox, order_requests, order_lines, orders CASCADE");
        INVENTORY_CALLS.set(0);
    }

    @Test void httpCreationStoresCompleteEventAndReplayKeepsOriginalPayload() throws Exception {
        Instant before = Instant.now();
        var response = send("origin-correlation");
        String id = mapper.readTree(response.getContentAsString()).path("id").textValue();
        assertThat(INVENTORY_CALLS.get()).isZero();
        assertCounts(1);
        var row = jdbc.queryForMap("SELECT * FROM order_outbox");
        String payload = (String) row.get("payload");
        var event = mapper.readTree(payload);
        assertThat(event.isObject()).isTrue();
        var fields = new java.util.ArrayList<String>();
        event.fieldNames().forEachRemaining(fields::add);
        assertThat(fields).containsExactlyInAnyOrder("eventId", "eventType", "schemaVersion", "occurredAt",
                "orderId", "correlationId", "lines");
        assertThat(event.path("eventId").isTextual()).isTrue();
        assertThat(UUID.fromString(event.path("eventId").textValue())).isEqualTo(row.get("event_id"));
        assertThat(event.path("occurredAt").isTextual()).isTrue();
        assertThat(event.path("occurredAt").textValue()).endsWith("Z");
        assertThat(Instant.parse(event.path("occurredAt").textValue())).isBetween(before, Instant.now());
        assertThat(event.path("eventType")).isEqualTo(mapper.getNodeFactory().textNode("OrderPlaced"));
        assertThat(event.path("schemaVersion").isIntegralNumber()).isTrue();
        assertThat(event.path("schemaVersion").intValue()).isEqualTo(1);
        assertThat(event.path("orderId")).isEqualTo(mapper.getNodeFactory().textNode(id));
        assertThat(event.path("correlationId")).isEqualTo(mapper.getNodeFactory().textNode("origin-correlation"));
        assertThat(event.path("lines")).isEqualTo(mapper.readTree(
                "[{\"sku\":\"B\",\"quantity\":2},{\"sku\":\"A\",\"quantity\":1}]"));
        assertThat(row.get("order_id")).isEqualTo(UUID.fromString(id));
        assertThat(row.get("message_key")).isEqualTo(id);
        assertThat(row.get("topic")).isEqualTo("commerce.orders.v1");
        assertThat(row.get("event_type")).isEqualTo("OrderPlaced");
        assertThat(row.get("schema_version")).isEqualTo(1);
        var replay = send("replay-correlation");
        assertThat(mapper.readTree(replay.getContentAsString()).path("id").textValue()).isEqualTo(id);
        assertCounts(1);
        assertThat(jdbc.queryForObject("SELECT payload FROM order_outbox", String.class)).isEqualTo(payload);
        assertThat(INVENTORY_CALLS.get()).isZero();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.MethodSource("missingOrInvalidCorrelations")
    void generatedCorrelationIsEchoedAndPersistedInRequestAndEvent(String incoming) throws Exception {
        var request = post("/api/v1/orders").header("Idempotency-Key", "generated")
                .contentType("application/json").content("""
                {"customerId":"cust","currency":"EUR",
                 "lines":[{"sku":"A","quantity":2,"unitPrice":1}]}
                """);
        if (incoming != null) request.header("X-Correlation-ID", incoming);
        var response = mvc.perform(request)
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING_INVENTORY"))
                .andReturn().getResponse();

        String echoed = response.getHeader("X-Correlation-ID");
        assertThat(echoed).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
                .isNotEqualTo(incoming);
        UUID orderId = UUID.fromString(mapper.readTree(response.getContentAsString()).path("id").textValue());
        assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
        assertCounts(1);
        assertThat(jdbc.queryForObject("SELECT correlation_id FROM order_requests WHERE order_id = ?",
                String.class, orderId)).isEqualTo(echoed);
        var event = mapper.readTree(jdbc.queryForObject("SELECT payload FROM order_outbox WHERE order_id = ?",
                String.class, orderId));
        assertThat(event.path("correlationId")).isEqualTo(mapper.getNodeFactory().textNode(echoed));
        assertThat(org.slf4j.MDC.get("correlationId")).isNull();
        assertThat(INVENTORY_CALLS.get()).isZero();
    }

    static java.util.stream.Stream<String> missingOrInvalidCorrelations() {
        return java.util.stream.Stream.of(null, "", "invalid id", "x".repeat(129), "bad\nheader");
    }

    @Test void noSynchronousInventoryOrRecoveryBeansExist() {
        for (String name : context.getBeanDefinitionNames()) {
            Class<?> type = context.getType(name);
            if (type != null) assertThat(type.getName()).doesNotContain("com.commercelab.order.inventory.",
                    "OrderReservationCoordinator", "OrderRecoveryWorker", "OrderRecoveryConfiguration", "OrderProgressService");
        }
    }

    @Test void failureAfterRealOutboxInsertRollsBackAllThreeRecords() {
        rollback(true);
    }

    @Test void failureBeforeOutboxInsertAfterIdentityRollsBackAllThreeRecords() {
        rollback(false);
    }

    private void rollback(boolean afterInsert) {
        doAnswer(call -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isTrue();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM order_requests", Long.class)).isEqualTo(1);
            if (afterInsert) call.callRealMethod();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM order_outbox", Long.class))
                    .isEqualTo(afterInsert ? 1L : 0L);
            throw new IllegalStateException("injected outbox failure");
        }).when(outbox).insert(any());
        assertThatThrownBy(() -> creation.createOrReplay("rollback", new PlaceOrderCommand("cust", "EUR",
                List.of(new PlaceOrderCommand.Line("A", 1, BigDecimal.ONE))), "c"))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("injected outbox failure");
        // No surrounding test transaction: JDBC reads committed database state, not JPA's cache.
        assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
        assertCounts(0);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_lines", Long.class)).isZero();
    }

    private void assertCounts(long count) {
        for (String table : List.of("orders", "order_requests", "order_outbox")) {
            assertThat(jdbc.queryForObject("SELECT count(*) FROM " + table, Long.class)).as(table).isEqualTo(count);
        }
    }

    private org.springframework.mock.web.MockHttpServletResponse send(String correlation) throws Exception {
        return mvc.perform(post("/api/v1/orders").header("Idempotency-Key", "creation")
                .header("X-Correlation-ID", correlation).contentType("application/json").content("""
                {"customerId":"cust","currency":"EUR","lines":[
                  {"sku":"B","quantity":2,"unitPrice":9.99},{"sku":"A","quantity":1,"unitPrice":1}]}
                """))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.status").value("PENDING_INVENTORY"))
                .andExpect(header().string("X-Correlation-ID", correlation))
                .andExpect(header().string("Retry-After", "5"))
                .andExpect(header().exists("Location")).andReturn().getResponse();
    }
}
