package com.commercelab.order.inventory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** HTTP fixture for the real gateway; only dynamic order identity is substituted. */
public final class InventoryContractFixture implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final HttpServer server;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private volatile JsonNode response;
    private final boolean echoOrderId;

    public InventoryContractFixture(boolean echoOrderId) {
        this.echoOrderId = echoOrderId;
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(executor);
            server.createContext("/api/v1/reservations", exchange -> {
                try (exchange) {
                    JsonNode request = JSON.readTree(exchange.getRequestBody());
                    JsonNode fixture = response;
                    ObjectNode body = fixture.path("body").deepCopy();
                    if (this.echoOrderId && body.has("orderId") && request != null) {
                        body.set("orderId", request.get("orderId"));
                    }
                    byte[] bytes = JSON.writeValueAsBytes(body);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(fixture.path("status").intValue(), bytes.length);
                    exchange.getResponseBody().write(bytes);
                }
            });
            respond("accepted");
            server.start();
        } catch (Exception ex) {
            executor.shutdownNow();
            throw new IllegalStateException(ex);
        }
    }

    public static JsonNode load(String name) throws Exception {
        try (var stream = InventoryContractFixture.class.getResourceAsStream("/contracts/inventory/v1/" + name + ".json")) {
            if (stream == null) throw new IllegalArgumentException("Missing inventory contract: " + name);
            return JSON.readTree(stream);
        }
    }

    public void respond(String name) throws Exception { response = load(name); }
    public void respond(JsonNode fixture) { response = fixture.deepCopy(); }
    public void unavailable() {
        response = JSON.createObjectNode().put("status", 503).set("body", JSON.createObjectNode());
    }
    public String url() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
    @Override public void close() { server.stop(0); executor.shutdownNow(); }
}
