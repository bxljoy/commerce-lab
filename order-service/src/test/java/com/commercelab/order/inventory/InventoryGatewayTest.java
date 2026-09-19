package com.commercelab.order.inventory;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.web.client.RestClient;

class InventoryGatewayTest {
    HttpServer server;
    java.util.concurrent.ExecutorService executor;
    org.apache.hc.client5.http.impl.classic.CloseableHttpClient client;
    RestInventoryGateway gateway;
    io.github.resilience4j.circuitbreaker.CircuitBreaker breaker;
    AtomicInteger calls;
    int status;
    String body;
    String correlation;
    boolean drop;
    long delay;
    long bodyDelay;
    java.util.concurrent.CountDownLatch arrived;
    java.util.concurrent.CountDownLatch hold;
    final UUID id = UUID.randomUUID();
    final InventoryRequest request = new InventoryRequest(id, List.of(new InventoryLine("A", 2)));

    @BeforeEach void setup() throws Exception {
        calls = new AtomicInteger();
        status = 201;
        body = success();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);
        server.createContext("/", exchange -> {
            calls.incrementAndGet();
            correlation = exchange.getRequestHeaders().getFirst("X-Correlation-ID");
            exchange.getRequestBody().readAllBytes();
            if (arrived != null) arrived.countDown();
            if (hold != null) {
                try { hold.await(3, java.util.concurrent.TimeUnit.SECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            try { Thread.sleep(delay); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            if (drop) { exchange.close(); return; }
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Location", "/redirect-target");
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            try { Thread.sleep(bodyDelay); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        var config = new InventoryClientConfiguration();
        client = config.inventoryHttpClient(100, 100, 100, 2, 1);
        breaker = config.inventoryCircuitBreaker();
        gateway = new RestInventoryGateway(RestClient.builder()
                .baseUrl("http://127.0.0.1:" + server.getAddress().getPort())
                .requestFactory(new org.springframework.http.client.HttpComponentsClientHttpRequestFactory(client))
                .build(), new ObjectMapper(), breaker);
    }

    @AfterEach void close() throws Exception {
        client.close(); server.stop(0); executor.shutdownNow();
    }

    String success() {
        return """
                {"orderId":"%s","status":"RESERVED","createdAt":"2026-09-19T12:00:00Z",
                 "lines":[{"sku":"A","quantity":2}]}
                """.formatted(id);
    }

    @Test void reservesAndPropagatesCorrelation() {
        assertThat(gateway.reserve(request, "origin:1")).isEqualTo(new InventoryOutcome.Reserved(id, request.lines()));
        assertThat(correlation).isEqualTo("origin:1");
        assertThat(calls).hasValue(1);
    }

    @Test void releasedReplayIsNotReserved() {
        status = 200; body = success().replace("RESERVED", "RELEASED");
        assertThat(gateway.reserve(request, "c")).isInstanceOf(InventoryOutcome.Released.class);
    }

    @Test void typedRejectionIsSuccessfulBreakerOutcome() {
        status = 409; body = rejection();
        for (int i = 0; i < 10; i++) assertThat(gateway.reserve(request, "c")).isInstanceOf(InventoryOutcome.Rejected.class);
        assertThat(breaker.getMetrics().getNumberOfFailedCalls()).isZero();
        assertThat(breaker.getState()).isEqualTo(io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED);
    }

    String rejection() {
        return """
                {"type":"https://commerce-lab/errors/stock-unavailable","unavailableSkus":{"A":{"requested":2,"available":0}}}
                """;
    }

    @Test void onlyTypedGet404MeansMissing() {
        status = 404; body = "{\"type\":\"https://commerce-lab/errors/reservation-not-found\"}";
        assertThat(gateway.find(id, "c")).isInstanceOf(InventoryOutcome.Missing.class);
        assertThatThrownBy(() -> gateway.reserve(request, "c")).isInstanceOf(InventoryProtocolException.class);
        body = "{\"type\":\"https://commerce-lab/errors/stock-not-found\"}";
        assertThatThrownBy(() -> gateway.find(id, "c")).isInstanceOf(InventoryProtocolException.class);
    }

    @ParameterizedTest @ValueSource(ints = {301, 302, 307, 308, 400, 401, 404, 409, 422, 429})
    void unexpectedStatusDoesNotRetryOrFollowRedirect(int code) {
        status = code; body = "{}";
        assertThatThrownBy(() -> gateway.reserve(request, "c")).isInstanceOf(InventoryProtocolException.class);
        assertThat(calls).hasValue(1);
    }

    @ParameterizedTest @ValueSource(strings = {"missing-id", "bad-id", "different-id", "missing-lines", "empty-lines",
            "duplicate", "wrong-sku", "wrong-quantity", "string-quantity", "fraction", "zero", "overflow",
            "missing-date", "bad-date", "bad-status", "null", "broken"})
    void rejectsMalformedSuccess(String mutation) {
        var json = new ObjectMapper();
        try {
            var root = (com.fasterxml.jackson.databind.node.ObjectNode) json.readTree(success());
            switch (mutation) {
                case "missing-id" -> root.remove("orderId");
                case "bad-id" -> root.put("orderId", "1-1-1-1-1");
                case "different-id" -> root.put("orderId", UUID.randomUUID().toString());
                case "missing-lines" -> root.remove("lines");
                case "empty-lines" -> root.putArray("lines");
                case "duplicate" -> ((com.fasterxml.jackson.databind.node.ArrayNode) root.get("lines")).add(root.get("lines").get(0).deepCopy());
                case "wrong-sku" -> ((com.fasterxml.jackson.databind.node.ObjectNode) root.at("/lines/0")).put("sku", "B");
                case "string-quantity" -> ((com.fasterxml.jackson.databind.node.ObjectNode) root.at("/lines/0")).put("quantity", "2");
                case "fraction" -> ((com.fasterxml.jackson.databind.node.ObjectNode) root.at("/lines/0")).put("quantity", 2.0);
                case "overflow" -> ((com.fasterxml.jackson.databind.node.ObjectNode) root.at("/lines/0")).put("quantity", 4294967298L);
                case "zero", "wrong-quantity" -> ((com.fasterxml.jackson.databind.node.ObjectNode) root.at("/lines/0")).put("quantity", mutation.equals("zero") ? 0 : 3);
                case "missing-date" -> root.remove("createdAt");
                case "bad-date" -> root.put("createdAt", "yesterday");
                case "bad-status" -> root.put("status", "CONFIRMED");
            }
            body = mutation.equals("null") ? "null" : mutation.equals("broken") ? "{" : root.toString();
        } catch (Exception e) { throw new AssertionError(e); }
        assertThatThrownBy(() -> gateway.reserve(request, "c")).isInstanceOf(InventoryProtocolException.class);
        assertThat(calls).hasValue(1);
    }

    @Test void timeoutAndDroppedSocketAreTransientWithoutImplicitRetry() {
        delay = 300;
        assertThatThrownBy(() -> gateway.reserve(request, "c")).isInstanceOf(TransientInventoryException.class);
        assertThat(calls).hasValue(1);
        delay = 0; drop = true;
        assertThatThrownBy(() -> gateway.reserve(request, "c")).isInstanceOf(TransientInventoryException.class);
        assertThat(calls).hasValue(2);
    }

    @Test void unreadable4xxBodyMustNotBecomeRetryableTransportFailure() {
        status = 409; body = rejection(); bodyDelay = 300;
        assertThatThrownBy(() -> gateway.reserve(request, "c")).isInstanceOf(InventoryProtocolException.class);
        assertThat(calls).hasValue(1);
    }

    @Test void matchesCompleteProblemTypeNotSuffix() {
        status = 409; body = rejection().replace("https://commerce-lab/errors/", "https://unrelated.example/");
        assertThatThrownBy(() -> gateway.reserve(request, "c")).isInstanceOf(InventoryProtocolException.class);
        assertThat(breaker.getMetrics().getNumberOfFailedCalls()).isZero();
    }

    @ParameterizedTest @ValueSource(strings = {"{}", "null", "[]",
            "{\"A\":{\"requested\":\"2\",\"available\":0}}",
            "{\"A\":{\"requested\":2,\"available\":2}}",
            "{\"A\":{\"requested\":3,\"available\":0}}",
            "{\"B\":{\"requested\":2,\"available\":0}}"})
    void malformedBusinessOutcomeDoesNotReject(String shortages) {
        status = 409;
        body = "{\"type\":\"https://commerce-lab/errors/stock-unavailable\",\"unavailableSkus\":" + shortages + "}";
        assertThatThrownBy(() -> gateway.reserve(request, "c")).isInstanceOf(InventoryProtocolException.class);
    }

    @ParameterizedTest @ValueSource(strings = {"A", "UNKNOWN"})
    void acceptsRequestedShortageSubset(String sku) {
        status = 409; body = rejection().replace("\"A\"", "\"" + sku + "\"");
        var two = new InventoryRequest(id, List.of(new InventoryLine(sku, 2), new InventoryLine("B", 1)));
        var expected = new InventoryOutcome.Rejected(java.util.Map.of(sku, new StockShortage(2, 0)));
        assertThat(gateway.reserve(two, "c")).isEqualTo(expected);
        assertThat(gateway.find(id, "c")).isEqualTo(expected);
    }

    @Test void acceptsEquivalentReorderedLines() {
        var two = new InventoryRequest(id, List.of(new InventoryLine("B", 1), new InventoryLine("A", 2)));
        body = success().replace("\"quantity\":2}", "\"quantity\":2},{\"sku\":\"B\",\"quantity\":1}");
        assertThat(gateway.reserve(two, "c")).isInstanceOf(InventoryOutcome.Reserved.class);
    }

    @Test void failedHalfOpenProbeReopensCircuit() {
        breaker.transitionToOpenState(); breaker.transitionToHalfOpenState();
        status = 503;
        for (int i = 0; i < 2; i++) assertThatThrownBy(() -> gateway.reserve(request, "c"))
                .isInstanceOf(TransientInventoryException.class);
        assertThat(breaker.getState()).isEqualTo(io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN);
    }

    @Test void exhaustedPoolHasBoundedAcquisitionWithoutAnotherSocket() throws Exception {
        client.close();
        client = new InventoryClientConfiguration().inventoryHttpClient(100, 100, 2000, 1, 1);
        gateway = new RestInventoryGateway(RestClient.builder()
                .baseUrl("http://127.0.0.1:" + server.getAddress().getPort())
                .requestFactory(new org.springframework.http.client.HttpComponentsClientHttpRequestFactory(client))
                .build(), new ObjectMapper(), breaker);
        arrived = new java.util.concurrent.CountDownLatch(1);
        hold = new java.util.concurrent.CountDownLatch(1);
        var first = executor.submit(() -> gateway.reserve(request, "c"));
        try {
            assertThat(arrived.await(1, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            var second = executor.submit(() -> catchThrowable(() -> gateway.reserve(request, "c")));
            assertThat(second.get(1, java.util.concurrent.TimeUnit.SECONDS)).isInstanceOf(TransientInventoryException.class);
            assertThat(calls).hasValue(1);
        } finally { hold.countDown(); }
        assertThat(first.get(1, java.util.concurrent.TimeUnit.SECONDS)).isInstanceOf(InventoryOutcome.Reserved.class);
    }

    @Test void breakerOpensSkipsNetworkAndClosesAfterHalfOpenProbes() {
        status = 503;
        for (int i = 0; i < 5; i++) assertThatThrownBy(() -> gateway.reserve(request, "c"))
                .isInstanceOf(TransientInventoryException.class);
        assertThat(breaker.getState()).isEqualTo(io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN);
        assertThatThrownBy(() -> gateway.reserve(request, "c")).isInstanceOf(TransientInventoryException.class);
        assertThat(calls).hasValue(5);
        breaker.transitionToHalfOpenState();
        status = 200;
        gateway.reserve(request, "c"); gateway.reserve(request, "c");
        assertThat(breaker.getState()).isEqualTo(io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED);
    }
}
