package com.commercelab.order.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

public final class EventJson {
    private static final Set<String> RESULT_FIELDS = Set.of("eventId", "eventType", "schemaVersion",
            "occurredAt", "orderId", "correlationId", "causationId", "lines");
    private static final Set<String> REJECTED_FIELDS = Set.of("eventId", "eventType", "schemaVersion",
            "occurredAt", "orderId", "correlationId", "causationId", "lines", "reasonCode", "shortages");

    private static final Set<String> LINE_FIELDS = Set.of("sku", "quantity");
    private static final Set<String> SHORTAGE_FIELDS = Set.of("sku", "requested", "available");
    private static final Pattern UUID_PATTERN = Pattern.compile(
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", Pattern.CASE_INSENSITIVE);
    private static final Pattern CORRELATION = Pattern.compile("[A-Za-z0-9._:-]{1,128}");
    private static final Pattern NONBLANK = Pattern.compile("\\S");
    private static final Pattern UTC_TIMESTAMP = Pattern.compile(
            "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,9})?(Z|\\+00:00)");

    // Event parsing is deliberately isolated from Spring's HTTP ObjectMapper.
    private final ObjectMapper mapper = JsonMapper.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .serializationInclusion(JsonInclude.Include.NON_NULL)
            .build();

    public InventoryResult readResult(String key, String value) {
        JsonNode node = content(value);
        validateKey(key, node);
        return bind(node, InventoryResult.class);
    }

    /** Validated original content for inbox equality; object order is ignored, array order is not. */
    public JsonNode content(String value) {
        require(value != null, "INVALID_JSON");
        JsonNode node;
        try {
            node = mapper.readTree(value);
        } catch (JsonProcessingException e) {
            // Jackson messages and causes can contain the rejected body.
            throw new EventProtocolException("INVALID_JSON");
        }
        require(node != null && node.isObject(), "INVALID_JSON");
        String type = text(node, "eventType");
        Set<String> fields = switch (type) {
            case "InventoryReserved" -> RESULT_FIELDS;
            case "InventoryRejected" -> REJECTED_FIELDS;

            default -> throw new EventProtocolException("UNSUPPORTED_EVENT_TYPE");
        };
        fields(node, fields);
        require(integer(node, "schemaVersion", 1) == 1, "UNSUPPORTED_SCHEMA_VERSION");
        uuid(text(node, "eventId"));
        uuid(text(node, "orderId"));
        if (node.has("causationId")) uuid(text(node, "causationId"));
        require(CORRELATION.matcher(text(node, "correlationId")).matches(), "INVALID_CORRELATION_ID");
        timestamp(text(node, "occurredAt"));
        Map<String, Integer> lines = lines(node.get("lines"));
        if (type.equals("InventoryRejected")) {
            require(text(node, "reasonCode").equals("INSUFFICIENT_STOCK"), "INVALID_REASON_CODE");
            shortages(node.get("shortages"), lines);
        }
        return node;
    }

    private static Map<String, Integer> lines(JsonNode node) {
        nonemptyArray(node);
        Map<String, Integer> lines = new HashMap<>();
        for (JsonNode line : node) {
            fields(line, LINE_FIELDS);
            String sku = sku(line);
            int quantity = integer(line, "quantity", 1);
            require(lines.putIfAbsent(sku, quantity) == null, "DUPLICATE_SKU");
        }
        return lines;
    }

    private static void shortages(JsonNode node, Map<String, Integer> lines) {
        nonemptyArray(node);
        Set<String> seen = new HashSet<>();
        for (JsonNode shortage : node) {
            fields(shortage, SHORTAGE_FIELDS);
            String sku = sku(shortage);
            int requested = integer(shortage, "requested", 1);
            int available = integer(shortage, "available", 0);
            require(seen.add(sku), "DUPLICATE_SKU");
            require(Integer.valueOf(requested).equals(lines.get(sku)) && available < requested,
                    "INVALID_SHORTAGE");
        }
    }

    private static void validateKey(String key, JsonNode node) {
        require(key != null && UUID_PATTERN.matcher(key).matches()
                && key.equals(UUID.fromString(key).toString()), "INVALID_KEY");
        require(key.equals(UUID.fromString(text(node, "orderId")).toString()), "KEY_ORDER_MISMATCH");
    }

    private static void uuid(String value) {
        require(UUID_PATTERN.matcher(value).matches(), "INVALID_UUID");
    }

    private static void timestamp(String value) {
        require(UTC_TIMESTAMP.matcher(value).matches(), "INVALID_TIMESTAMP");
        try {
            OffsetDateTime.parse(value);
        } catch (DateTimeParseException e) {
            throw new EventProtocolException("INVALID_TIMESTAMP");
        }
    }

    private static String sku(JsonNode node) {
        String value = text(node, "sku");
        require(value.codePointCount(0, value.length()) <= 64 && NONBLANK.matcher(value).find(), "INVALID_SKU");
        return value;
    }

    private static int integer(JsonNode node, String field, int minimum) {
        JsonNode value = node.get(field);
        require(value != null && value.isIntegralNumber() && value.canConvertToInt()
                && value.intValue() >= minimum, "INVALID_INTEGER");
        return value.intValue();
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        require(value != null && value.isTextual(), "INVALID_FIELD_TYPE");
        return value.textValue();
    }

    private static void fields(JsonNode node, Set<String> expected) {
        require(node != null && node.isObject() && node.size() == expected.size(), "INVALID_FIELDS");
        for (String field : expected) require(node.hasNonNull(field), "INVALID_FIELDS");
    }

    private static void nonemptyArray(JsonNode node) {
        require(node != null && node.isArray() && !node.isEmpty(), "INVALID_ARRAY");
    }

    private <T> T bind(JsonNode node, Class<T> type) {
        try {
            return mapper.treeToValue(node, type);
        } catch (JsonProcessingException e) {
            throw new EventProtocolException("INVALID_EVENT");
        }
    }

    private static void require(boolean valid, String code) {
        if (!valid) throw new EventProtocolException(code);
    }
}
