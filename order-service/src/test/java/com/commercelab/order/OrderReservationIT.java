package com.commercelab.order;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.commercelab.order.inventory.InventoryGateway;
import com.commercelab.order.service.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.*;

@SpringBootTest
@AutoConfigureMockMvc
class OrderReservationIT extends AbstractPostgresIntegrationTest {
    static HttpServer server;
    static ExecutorService sockets;
    static volatile int responseStatus = 201;
    static volatile String mode = "success";
    static volatile String correlation;
    static volatile CountDownLatch entered;
    static volatile CountDownLatch release;
    static AtomicInteger calls = new AtomicInteger();
    static ObjectMapper json = new ObjectMapper();
    static {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            sockets = Executors.newVirtualThreadPerTaskExecutor();
            server.setExecutor(sockets);
            server.createContext("/api/v1/reservations", exchange -> {
                int call = calls.incrementAndGet();
                correlation = exchange.getRequestHeaders().getFirst("X-Correlation-ID");
                var request = json.readTree(exchange.getRequestBody());
                if (mode.equals("drop")) { exchange.close(); return; }
                if (entered != null) entered.countDown();
                if (release != null) {
                    try { release.await(5, TimeUnit.SECONDS); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }
                int status = mode.equals("retry") && call == 1 ? 503 : responseStatus;
                var body = json.createObjectNode();
                if (status == 409) {
                    body.put("type", "https://commerce-lab/errors/stock-unavailable");
                    body.putObject("unavailableSkus").putObject("A").put("requested", 2).put("available", 0);
                } else {
                    body.set("orderId", request.get("orderId"));
                    body.set("lines", request.get("lines"));
                    body.put("status", mode.equals("released") ? "RELEASED" : "RESERVED");
                    body.put("createdAt", "2026-09-19T12:00:00Z");
                    if (mode.equals("mismatch")) body.put("orderId", UUID.randomUUID().toString());
                }
                byte[] bytes = json.writeValueAsBytes(body);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(status, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            server.start();
        } catch (Exception e) { throw new ExceptionInInitializerError(e); }
    }
    @DynamicPropertySource static void inventory(DynamicPropertyRegistry registry) {
        registry.add("inventory.base-url", () -> "http://127.0.0.1:" + server.getAddress().getPort());
    }
    @AfterAll static void shutdown() { server.stop(0); sockets.shutdownNow(); }
    @Autowired MockMvc mvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired OrderCreationService creation;
    @Autowired OrderReservationCoordinator coordinator;
    @Autowired PlatformTransactionManager transactions;
    @Autowired io.github.resilience4j.circuitbreaker.CircuitBreaker breaker;
    @SpyBean InventoryGateway gateway;
    AtomicInteger probes;
    static final String BODY = """
            {"customerId":"private-customer","currency":"EUR","lines":[{"sku":"A","quantity":2,"unitPrice":1}]}
            """;

    @BeforeEach void reset() {
        jdbc.execute("TRUNCATE order_requests, order_lines, orders CASCADE");
        calls.set(0); probes = new AtomicInteger(); responseStatus = 201; mode = "success";
        entered = null; release = null; breaker.reset();
        doAnswer(invocation -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            probes.incrementAndGet();
            return invocation.callRealMethod();
        }).when(gateway).reserve(any(), any());
    }

    @Test void knownSuccessAndTerminalReplay() throws Exception {
        send("known").andExpect(status().isCreated()).andExpect(jsonPath("$.status").value("CONFIRMED"))
                .andExpect(header().exists("Location")).andExpect(header().doesNotExist("Retry-After"));
        send("known").andExpect(status().isOk()).andExpect(jsonPath("$.status").value("CONFIRMED"));
        assertThat(calls).hasValue(1); assertThat(probes).hasValue(1);
        assertThat(correlation).isEqualTo("request:origin");
    }

    @Test void stockRejectionIs201WithoutRetryAndReplays200() throws Exception {
        responseStatus = 409;
        send("reject").andExpect(status().isCreated()).andExpect(jsonPath("$.status").value("REJECTED"))
                .andExpect(jsonPath("$.rejectionReason").value("STOCK_UNAVAILABLE"));
        send("reject").andExpect(status().isOk());
        assertThat(calls).hasValue(1);
    }

    @Test void twoTransientCallsThenPendingReplayNeverCallsGateway() throws Exception {
        responseStatus = 503;
        send("pending").andExpect(status().isAccepted()).andExpect(header().string("Retry-After", "5"));
        responseStatus = 201;
        send("pending").andExpect(status().isAccepted()).andExpect(jsonPath("$.status").value("PENDING_INVENTORY"));
        assertThat(calls).hasValue(2); assertThat(probes).hasValue(2);
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM orders", Integer.class)).isEqualTo(1);
    }

    @Test void retryCanResolveSecondPhysicalCall() throws Exception {
        mode = "retry";
        send("retry").andExpect(status().isCreated()).andExpect(jsonPath("$.status").value("CONFIRMED"));
        assertThat(calls).hasValue(2);
    }

    @Test void droppedResponsesStopAtTwoPosts() throws Exception {
        mode = "drop";
        send("dropped").andExpect(status().isAccepted());
        assertThat(calls).hasValue(2);
    }

    @Test void generatedCorrelationIsEchoedPersistedAndPropagated() throws Exception {
        var response = mvc.perform(post("/api/v1/orders").header("Idempotency-Key", "generated")
                .header("X-Correlation-ID", "invalid id").contentType("application/json").content(BODY))
                .andExpect(status().isCreated()).andReturn().getResponse();
        String echoed = response.getHeader("X-Correlation-ID");
        assertThat(echoed).matches("[0-9a-f-]{36}").isEqualTo(correlation);
        assertThat(jdbc.queryForObject("SELECT correlation_id FROM order_requests", String.class)).isEqualTo(echoed);
        assertThat(org.slf4j.MDC.get("correlationId")).isNull();
    }

    @Test void activeCallerTransactionIsRejectedBeforeGatewayEntry() {
        var command = new PlaceOrderCommand("cust", "EUR", java.util.List.of(
                new PlaceOrderCommand.Line("A", 2, java.math.BigDecimal.ONE)));
        UUID id = creation.createOrReplay("transaction", command, "c").order().id();
        assertThatThrownBy(() -> new TransactionTemplate(transactions).execute(tx -> coordinator.attempt(id)))
                .isInstanceOf(org.springframework.transaction.IllegalTransactionStateException.class);
        assertThat(calls).hasValue(0); assertThat(probes).hasValue(0);
    }

    @Test void protocolAndReleasedBlockWithoutRetry() throws Exception {
        for (String scenario : java.util.List.of("mismatch", "released", "unexpected")) {
            mode = scenario; responseStatus = scenario.equals("unexpected") ? 400 : 200;
            send(scenario).andExpect(status().isAccepted()).andExpect(jsonPath("$.recoveryIssue").isNotEmpty());
        }
        assertThat(calls).hasValue(3);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM orders WHERE recovery_blocked", Integer.class)).isEqualTo(3);
    }

    @Test void openCircuitMakesNoHttpCall() throws Exception {
        breaker.transitionToOpenState();
        send("open").andExpect(status().isAccepted());
        assertThat(calls).hasValue(0);
    }

    @Test void blockedHttpDoesNotHoldOrderRowOrCallerTransaction() throws Exception {
        var command = new PlaceOrderCommand("cust", "EUR", java.util.List.of(
                new PlaceOrderCommand.Line("A", 2, java.math.BigDecimal.ONE)));
        UUID id = creation.createOrReplay("blocking", command, null).order().id();
        entered = new CountDownLatch(1); release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var result = executor.submit(() -> {
                try { return coordinator.attempt(id); }
                finally { assertThat(org.slf4j.MDC.get("correlationId")).isNull(); }
            });
            try {
                assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
                var other = executor.submit(() -> new TransactionTemplate(transactions).execute(tx -> {
                    jdbc.execute("SET LOCAL lock_timeout = '500ms'");
                    return jdbc.update("UPDATE orders SET version = version + 1 WHERE id = ?", id);
                }));
                assertThat(other.get(1, TimeUnit.SECONDS)).isEqualTo(1);
            } finally { release.countDown(); }
            assertThat(result.get(3, TimeUnit.SECONDS).status().name()).isEqualTo("CONFIRMED");
        }
        assertThat(correlation).matches("[0-9a-f-]{36}");
        assertThat(probes).hasValue(1);
    }

    org.springframework.test.web.servlet.ResultActions send(String key) throws Exception {
        return mvc.perform(post("/api/v1/orders").header("Idempotency-Key", key)
                .header("X-Correlation-ID", "request:origin")
                .contentType("application/json").content(BODY));
    }
}
