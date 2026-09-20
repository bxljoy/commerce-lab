package com.commercelab.inventory.events;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.function.Consumer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class EventContractTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final EventJson decoder = new EventJson();

    private String fixture(String name) throws IOException {
        try (var input = getClass().getResourceAsStream("/contracts/events/" + name + ".json")) {
            assertNotNull(input, name);
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private List<String> fixtures() {
        return List.of("orders/v1/order-placed", "inventory/v1/inventory-reserved", "inventory/v1/inventory-rejected");
    }

    private Object read(String name, String key, String value) {
        if (name.startsWith("orders/")) return decoder.readOrderPlaced(key, value);
        return decoder.readResult(key, value);
    }

    private void rejects(String name, String key, String value) {
        var failure = assertThrows(EventProtocolException.class, () -> read(name, key, value));
        assertNotNull(failure.code());
        assertFalse(failure.code().isBlank());
        assertEquals(failure.code(), failure.getMessage());
        assertNull(failure.getCause(), "Parser causes can leak raw JSON");
    }

    @TestFactory
    List<DynamicTest> invalidMutations() throws Exception {
        var tests = new ArrayList<DynamicTest>();
        for (String name : fixtures()) {
            ObjectNode original = (ObjectNode) mapper.readTree(fixture(name));
            String key = original.get("orderId").textValue();
            var mutations = new LinkedHashMap<String, Consumer<ObjectNode>>();
            original.fieldNames().forEachRemaining(field -> {
                mutations.put("missing " + field, n -> n.remove(field));
                mutations.put("null " + field, n -> n.putNull(field));
                mutations.put("wrong type " + field, n -> n.put(field, false));
            });
            mutations.put("unknown field", n -> n.put("privateData", "must-not-leak"));
            mutations.put("unsupported type", n -> n.put("eventType", "OtherEvent"));
            for (String field : List.of("eventId", "orderId", "causationId")) {
                if (!original.has(field)) continue;
                for (String bad : List.of("not-a-uuid", "1-1-1-1-1", "07e71c02-8c9e-4571-b16f-c7020e19f68", key + " ")) {
                    mutations.put(field + ":" + bad, n -> n.put(field, bad));
                }
            }
            for (String json : List.of("0", "-1", "2", "1.0", "\"1\"", "4294967297")) {
                JsonNode bad = mapper.readTree(json);
                mutations.put("version " + json, n -> n.set("schemaVersion", bad));
            }
            for (String bad : List.of("", "2026-02-30T12:00:00Z", "2026-09-20", "yesterday",
                    "2026-09-20T12:00:00", "2026-09-20T12:00:00+25:00", "2026-09-20T24:00:00Z")) {
                mutations.put("timestamp " + bad, n -> n.put("occurredAt", bad));
            }
            if (!name.startsWith("orders/")) {
                mutations.put("non-UTC result", n -> n.put("occurredAt", "2026-09-20T12:00:00+01:00"));
            }
            for (String bad : List.of("", " ", "has space", "has\nnewline", "a".repeat(129), "\u00e9")) {
                mutations.put("correlation " + bad, n -> n.put("correlationId", bad));
            }
            mutations.put("empty lines", n -> n.putArray("lines"));
            mutations.put("non-object line", n -> n.withArray("lines").add(1));
            mutations.put("duplicate SKU", n -> n.withArray("lines").add(n.at("/lines/0").deepCopy()));
            for (String field : List.of("sku", "quantity")) {
                mutations.put("missing line " + field, n -> ((ObjectNode) n.at("/lines/0")).remove(field));
                mutations.put("null line " + field, n -> ((ObjectNode) n.at("/lines/0")).putNull(field));
            }
            mutations.put("unknown line field", n -> ((ObjectNode) n.at("/lines/0")).put("extra", 1));
            for (String bad : List.of("", " ", "\t\n", "a".repeat(65))) {
                mutations.put("sku " + bad, n -> ((ObjectNode) n.at("/lines/0")).put("sku", bad));
            }
            mutations.put("numeric sku", n -> ((ObjectNode) n.at("/lines/0")).put("sku", 1));
            for (String json : List.of("0", "-1", "1.0", "1e0", "\"1\"", "2147483648", "4294967297", "true")) {
                JsonNode bad = mapper.readTree(json);
                mutations.put("quantity " + json, n -> ((ObjectNode) n.at("/lines/0")).set("quantity", bad));
            }
            if (original.has("shortages")) {
                mutations.put("empty shortages", n -> n.putArray("shortages"));
                mutations.put("duplicate shortage", n -> n.withArray("shortages").add(n.at("/shortages/0").deepCopy()));
                mutations.put("non-object shortage", n -> n.withArray("shortages").add(1));
                mutations.put("unknown shortage sku", n -> ((ObjectNode) n.at("/shortages/0")).put("sku", "OTHER"));
                mutations.put("unknown shortage field", n -> ((ObjectNode) n.at("/shortages/0")).put("extra", 1));
                mutations.put("wrong requested", n -> ((ObjectNode) n.at("/shortages/0")).put("requested", 1));
                mutations.put("available equals requested", n -> ((ObjectNode) n.at("/shortages/0")).put("available", 2));
                mutations.put("available exceeds requested", n -> ((ObjectNode) n.at("/shortages/0")).put("available", 3));
                mutations.put("wrong reason", n -> n.put("reasonCode", "OTHER"));
                for (String field : List.of("sku", "requested", "available")) {
                    mutations.put("missing shortage " + field, n -> ((ObjectNode) n.at("/shortages/0")).remove(field));
                    mutations.put("null shortage " + field, n -> ((ObjectNode) n.at("/shortages/0")).putNull(field));
                }
                for (String field : List.of("requested", "available")) {
                    for (String json : List.of("-1", "1.0", "\"1\"", "2147483648", "4294967297", "true")) {
                        JsonNode bad = mapper.readTree(json);
                        mutations.put(field + " " + json, n -> ((ObjectNode) n.at("/shortages/0")).set(field, bad));
                    }
                }
                mutations.put("zero requested", n -> ((ObjectNode) n.at("/shortages/0")).put("requested", 0));
            } else {
                mutations.put("unexpected reason", n -> n.put("reasonCode", "INSUFFICIENT_STOCK"));
                mutations.put("unexpected shortages", n -> n.putArray("shortages"));
                mutations.put("null unexpected reason", n -> n.putNull("reasonCode"));
                mutations.put("null unexpected shortages", n -> n.putNull("shortages"));
            }
            mutations.forEach((label, mutation) -> tests.add(DynamicTest.dynamicTest(name + ": " + label, () -> {
                ObjectNode changed = original.deepCopy();
                mutation.accept(changed);
                rejects(name, key, changed.toString());
                assertThrows(EventProtocolException.class, () -> decoder.content(changed.toString()));
            })));
            for (String bad : new String[]{null, "", "null", "[]", "42", "true", "{",
                    original + " {}", original.toString().replaceFirst("\\{", "{\"eventId\":\"secret\","),
                    original.toString().replace("\"quantity\":2", "\"quantity\":2,\"quantity\":2")}) {
                tests.add(DynamicTest.dynamicTest(name + ": malformed " + bad, () -> {
                    rejects(name, key, bad);
                    assertThrows(EventProtocolException.class, () -> decoder.content(bad));
                }));
            }
            for (String badKey : new String[]{null, "", key.toUpperCase(), "1-1-1-1-1", key + " ",
                    "11111111-1111-1111-1111-111111111111"}) {
                tests.add(DynamicTest.dynamicTest(name + ": key " + badKey,
                        () -> rejects(name, badKey, original.toString())));
            }
        }
        return tests;
    }

    @Test
    void fixturesPreserveIdentityAndOrderedLines() throws Exception {
        JsonNode placed = mapper.readTree(fixture("orders/v1/order-placed"));
        for (String name : List.of("inventory/v1/inventory-reserved", "inventory/v1/inventory-rejected")) {
            String value = fixture(name);
            var result = decoder.readResult(placed.get("orderId").textValue(), value);
            assertEquals(placed.get("orderId").textValue(), result.orderId().toString());
            assertEquals(placed.get("eventId").textValue(), result.causationId().toString());
            assertEquals(placed.get("correlationId").textValue(), result.correlationId());
            assertEquals(List.of(new InventoryResult.Line("SKU-B", 2), new InventoryResult.Line("SKU-A", 1)), result.lines());
            assertEquals(name.endsWith("reserved") ? "InventoryReserved" : "InventoryRejected", result.eventType());
            assertEquals(1, result.schemaVersion());
            assertEquals("2026-09-20T12:34:56Z", result.occurredAt().toString());
            assertNotEquals(result.causationId(), result.eventId());
            assertThrows(UnsupportedOperationException.class, () -> result.lines().clear());
            if (name.endsWith("reserved")) {
                assertNull(result.reasonCode());
                assertNull(result.shortages());
            } else {
                assertEquals("INSUFFICIENT_STOCK", result.reasonCode());
                assertEquals(List.of(new InventoryResult.Shortage("SKU-B", 2, 0)), result.shortages());
                assertThrows(UnsupportedOperationException.class, () -> result.shortages().clear());
            }
        }
    }

    @Test
    void contentIgnoresObjectOrderButPreservesArrayOrder() throws Exception {
        for (String name : fixtures()) {
            ObjectNode original = (ObjectNode) mapper.readTree(fixture(name));
            ObjectNode reordered = mapper.createObjectNode();
            var fields = new ArrayList<String>();
            original.fieldNames().forEachRemaining(fields::add);
            java.util.Collections.reverse(fields);
            fields.forEach(f -> reordered.set(f, original.get(f).deepCopy()));
            ObjectNode line = (ObjectNode) reordered.at("/lines/0");
            int quantity = line.remove("quantity").intValue();
            String sku = line.remove("sku").textValue();
            line.put("quantity", quantity).put("sku", sku);
            assertEquals(decoder.content(original.toString()), decoder.content(reordered.toString()));
            JsonNode first = reordered.withArray("lines").remove(0);
            reordered.withArray("lines").add(first);
            assertNotEquals(decoder.content(original.toString()), decoder.content(reordered.toString()));
        }
    }

    @Test
    void payloadUuidCaseDoesNotChangeIdentityButDoesChangeInboxContent() throws Exception {
        for (String name : fixtures()) {
            ObjectNode original = (ObjectNode) mapper.readTree(fixture(name));
            String key = original.get("orderId").textValue();
            for (boolean mixedCase : List.of(false, true)) {
                ObjectNode changed = original.deepCopy();
                for (String field : List.of("eventId", "orderId", "causationId")) {
                    if (!changed.has(field)) continue;
                    String uuid = changed.get(field).textValue();
                    changed.put(field, mixedCase
                            ? uuid.substring(0, 18).toUpperCase(java.util.Locale.ROOT) + uuid.substring(18)
                            : uuid.toUpperCase(java.util.Locale.ROOT));
                }
                assertEquals(read(name, key, original.toString()), read(name, key, changed.toString()));
                assertNotEquals(decoder.content(original.toString()), decoder.content(changed.toString()));
                rejects(name, key.toUpperCase(java.util.Locale.ROOT), changed.toString());
            }
        }
    }

    @Test
    void acceptsValidBoundaryValues() throws Exception {
        for (String name : fixtures()) {
            ObjectNode value = (ObjectNode) mapper.readTree(fixture(name));
            value.put("correlationId", "a".repeat(128));
            value.put("occurredAt", "2026-09-20T12:34:56.123456789+00:00");
            ((ObjectNode) value.at("/lines/1")).put("sku", "a".repeat(64)).put("quantity", Integer.MAX_VALUE);
            if (value.has("shortages")) ((ObjectNode) value.at("/shortages/0")).put("available", 1);
            assertNotNull(read(name, value.get("orderId").textValue(), value.toString()));
        }
    }

    @Test
    void orderPlacedDecodesAndReadersEnforceType() throws Exception {
        String value = fixture("orders/v1/order-placed");
        String key = mapper.readTree(value).get("orderId").textValue();
        var placed = decoder.readOrderPlaced(key, value);
        assertEquals(List.of(new OrderPlaced.Line("SKU-B", 2), new OrderPlaced.Line("SKU-A", 1)), placed.lines());
        assertThrows(UnsupportedOperationException.class, () -> placed.lines().clear());
        assertEquals("OrderPlaced", placed.eventType());
        assertEquals("2026-09-19T12:34:56Z", placed.occurredAt().toString());
        assertThrows(EventProtocolException.class, () -> decoder.readResult(key, value));
        assertThrows(EventProtocolException.class, () -> decoder.readOrderPlaced(key, fixture("inventory/v1/inventory-reserved")));
    }

    @Test
    void orderPlacedAcceptsRfc3339OffsetsWithoutNormalizingInboxContent() throws Exception {
        String original = fixture("orders/v1/order-placed");
        String key = mapper.readTree(original).get("orderId").textValue();
        var expected = decoder.readOrderPlaced(key, original);
        for (String timestamp : List.of("2026-09-19T14:34:56+02:00", "2026-09-19T07:04:56-05:30",
                "2026-09-19t12:34:56z")) {
            ObjectNode changed = (ObjectNode) mapper.readTree(original);
            changed.put("occurredAt", timestamp);
            assertEquals(expected, decoder.readOrderPlaced(key, changed.toString()));
            assertEquals(timestamp, decoder.content(changed.toString()).get("occurredAt").textValue());
            assertNotEquals(decoder.content(original), decoder.content(changed.toString()));
        }
    }

    @Test
    void serializerEmitsUtcForAlternativeUtcInputRepresentation() throws Exception {
        ObjectNode value = (ObjectNode) mapper.readTree(fixture("inventory/v1/inventory-reserved"));
        value.put("occurredAt", "2026-09-20T12:34:56+00:00");
        var result = decoder.readResult(value.get("orderId").textValue(), value.toString());
        String written = decoder.writeResult(result);
        assertEquals("2026-09-20T12:34:56Z", mapper.readTree(written).get("occurredAt").textValue());
        assertNotEquals(decoder.content(value.toString()), decoder.content(written));
    }

    @Test
    void serializerRoundTripsWithoutNullSuccessFields() throws Exception {
        for (String name : List.of("inventory/v1/inventory-reserved", "inventory/v1/inventory-rejected")) {
            String value = fixture(name);
            String key = mapper.readTree(value).get("orderId").textValue();
            var result = decoder.readResult(key, value);
            String written = decoder.writeResult(result);
            assertEquals(decoder.content(value), decoder.content(written));
            assertEquals(result, decoder.readResult(key, written));
        }
    }

}
