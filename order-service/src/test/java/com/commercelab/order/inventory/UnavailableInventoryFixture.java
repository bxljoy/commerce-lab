package com.commercelab.order.inventory;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;

/** Explicit local failure, independent of any inventory process on the developer machine. */
public final class UnavailableInventoryFixture implements AutoCloseable {
    private final HttpServer server;

    public UnavailableInventoryFixture() {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                exchange.getRequestBody().readAllBytes();
                exchange.sendResponseHeaders(503, -1);
                exchange.close();
            });
            server.start();
        } catch (IOException ex) { throw new IllegalStateException(ex); }
    }

    public String url() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
    @Override public void close() { server.stop(0); }
}
