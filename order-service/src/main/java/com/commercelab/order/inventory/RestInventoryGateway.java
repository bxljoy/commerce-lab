package com.commercelab.order.inventory;

import com.commercelab.order.web.CorrelationIdFilter;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

/** One physical HTTP call per method; retry belongs to the coordinator. */
public class RestInventoryGateway implements InventoryGateway {
    private static final Logger log = LoggerFactory.getLogger(RestInventoryGateway.class);
    private static final String PROBLEM = "https://commerce-lab/errors/";
    private final RestClient client;
    private final ObjectReader reader;
    private final CircuitBreaker breaker;

    public RestInventoryGateway(RestClient client, ObjectMapper mapper, CircuitBreaker breaker) {
        this.client = client;
        this.reader = mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .with(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        this.breaker = breaker;
    }

    @Override
    public InventoryOutcome reserve(InventoryRequest request, String correlationId) {
        String correlation = CorrelationIdFilter.validOrNew(correlationId);
        return call(request.orderId(), correlation, "reserve", () -> client.post().uri("/api/v1/reservations")
                .contentType(MediaType.APPLICATION_JSON).header(CorrelationIdFilter.HEADER, correlation)
                .body(request).exchange((req, response) -> decode(response.getStatusCode().value(),
                        response.getBody(), request.orderId(), request)));
    }

    @Override
    public InventoryOutcome find(UUID orderId, String correlationId) {
        String correlation = CorrelationIdFilter.validOrNew(correlationId);
        return call(orderId, correlation, "find", () -> client.get().uri("/api/v1/reservations/{id}", orderId)
                .header(CorrelationIdFilter.HEADER, correlation)
                .exchange((req, response) -> decode(response.getStatusCode().value(),
                        response.getBody(), orderId, null)));
    }

    private InventoryOutcome call(UUID orderId, String correlation, String operation, Supplier<InventoryOutcome> action) {
        long start = System.nanoTime();
        String failure = "NONE";
        try {
            return breaker.executeSupplier(() -> {
                try { return action.get(); }
                catch (ResourceAccessException ex) { throw new TransientInventoryException("INVENTORY_TRANSPORT"); }
            });
        } catch (CallNotPermittedException ex) {
            failure = "INVENTORY_CIRCUIT_OPEN";
            throw new TransientInventoryException(failure);
        } catch (TransientInventoryException ex) {
            failure = ex.code(); throw ex;
        } catch (InventoryProtocolException ex) {
            failure = ex.code(); throw ex;
        } finally {
            log.info("orderId={} correlationId={} operation={} latencyMs={} failureCode={}",
                    orderId, correlation, operation, (System.nanoTime() - start) / 1_000_000, failure);
        }
    }

    private InventoryOutcome decode(int status, java.io.InputStream stream, UUID expectedId,
            InventoryRequest request) throws java.io.IOException {
        if (status >= 500 && status <= 599) throw new TransientInventoryException("INVENTORY_SERVER_ERROR");
        if (status != 200 && status != 201 && status != 409 && status != 404) {
            throw new InventoryProtocolException("INVENTORY_UNEXPECTED_STATUS");
        }
        JsonNode body;
        try { body = reader.readTree(stream); }
        catch (JsonProcessingException ex) { throw invalid(); }
        catch (java.io.IOException ex) {
            // Once a 4xx is observed, even an unreadable problem body must not trigger retry.
            if (status >= 400 && status < 500) throw invalid();
            throw ex;
        }
        if (body == null || !body.isObject()) throw invalid();
        if (status == 409 || status == 404) {
            String type = text(body, "type");
            if (status == 404 && request == null && type.equals(PROBLEM + "reservation-not-found")) {
                return new InventoryOutcome.Missing();
            }
            if (status == 409 && type.equals(PROBLEM + "stock-unavailable")) return rejection(body, request);
            throw new InventoryProtocolException(type.equals(PROBLEM + "reservation-payload-conflict")
                    ? "INVENTORY_PAYLOAD_CONFLICT" : "INVENTORY_UNEXPECTED_STATUS");
        }
        if (request == null && status != 200) throw new InventoryProtocolException("INVENTORY_UNEXPECTED_STATUS");
        UUID orderId;
        try {
            String value = text(body, "orderId");
            orderId = UUID.fromString(value);
            if (!orderId.toString().equalsIgnoreCase(value)) throw invalid();
            OffsetDateTime.parse(text(body, "createdAt"));
            if (body.hasNonNull("releasedAt")) OffsetDateTime.parse(text(body, "releasedAt"));
        } catch (IllegalArgumentException | java.time.format.DateTimeParseException ex) { throw invalid(); }
        if (!expectedId.equals(orderId)) throw new InventoryProtocolException("INVENTORY_RESPONSE_MISMATCH");
        JsonNode rawLines = body.path("lines");
        if (!rawLines.isArray() || rawLines.isEmpty()) throw invalid();
        var lines = new ArrayList<InventoryLine>();
        for (JsonNode line : rawLines) lines.add(new InventoryLine(text(line, "sku"), integer(line, "quantity", 1)));
        InventoryResponseValidation.lineMap(lines);
        if (request != null) InventoryResponseValidation.requireMatch(request, orderId, lines);
        return switch (text(body, "status")) {
            case "RESERVED" -> new InventoryOutcome.Reserved(orderId, lines);
            case "RELEASED" -> new InventoryOutcome.Released(orderId, lines);
            default -> throw invalid();
        };
    }

    private InventoryOutcome rejection(JsonNode body, InventoryRequest request) {
        JsonNode unavailable = body.path("unavailableSkus");
        if (!unavailable.isObject() || unavailable.isEmpty()) throw invalid();
        var shortages = new HashMap<String, StockShortage>();
        var expected = request == null ? null : InventoryResponseValidation.lineMap(request.lines());
        unavailable.fields().forEachRemaining(entry -> {
            String sku = entry.getKey();
            int requested = integer(entry.getValue(), "requested", 1);
            int available = integer(entry.getValue(), "available", 0);
            if (sku.isBlank() || sku.length() > 64 || available >= requested
                    || (expected != null && !Integer.valueOf(requested).equals(expected.get(sku)))) throw invalid();
            shortages.put(sku, new StockShortage(requested, available));
        });
        return new InventoryOutcome.Rejected(shortages);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isTextual() || value.textValue().isBlank()) throw invalid();
        return value.textValue();
    }

    private static int integer(JsonNode node, String field, int minimum) {
        JsonNode value = node.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < minimum) throw invalid();
        return value.intValue();
    }

    private static InventoryProtocolException invalid() {
        return new InventoryProtocolException("INVENTORY_INVALID_RESPONSE");
    }
}
