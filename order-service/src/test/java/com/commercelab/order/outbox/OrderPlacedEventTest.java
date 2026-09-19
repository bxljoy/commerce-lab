package com.commercelab.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.commercelab.order.domain.Money;
import com.commercelab.order.domain.Order;
import com.commercelab.order.domain.OrderLine;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Currency;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class OrderPlacedEventTest {
    private final Clock clock = Clock.fixed(Instant.parse("2026-09-19T12:34:56Z"), ZoneOffset.UTC);
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final OrderPlacedEventFactory factory = new OrderPlacedEventFactory(clock, mapper);

    @Test
    void createsContractEnvelopeWithStableSnapshotAndCreationTimestamp() throws Exception {
        Order order = order();
        OutboxMessage message = factory.create(order, "phase4-proof");
        JsonNode body = mapper.readTree(message.payload());
        assertContract(body);
        assertThat(body.get("eventId").asText()).isEqualTo(message.eventId().toString());
        assertThat(body.get("orderId").asText()).isEqualTo(order.id().toString());
        assertThat(message.orderId()).isEqualTo(order.id());
        assertThat(message.messageKey()).isEqualTo(order.id().toString());
        assertThat(message.topic()).isEqualTo("commerce.orders.v1");
        assertThat(body.get("occurredAt").asText()).isEqualTo("2026-09-19T12:34:56Z");
        assertThat(body.get("correlationId").asText()).isEqualTo("phase4-proof");
        assertThat(body.get("lines").get(0).get("sku").asText()).isEqualTo("SKU-B");
        assertThat(body.get("lines").get(0).get("quantity").intValue()).isEqualTo(2);
        assertThat(body.get("lines").get(1).get("sku").asText()).isEqualTo("SKU-A");
        assertThat(body.has("customerId")).isFalse();
        assertThat(factory.create(order, "phase4-proof").eventId()).isNotEqualTo(message.eventId());
    }

    @Test
    void envelopeDefensivelyCopiesOrderedLines() {
        var lines = new ArrayList<>(List.of(new OrderPlacedEvent.Line("SKU-B", 2),
                new OrderPlacedEvent.Line("SKU-A", 1)));
        var event = new OrderPlacedEvent(UUID.randomUUID(), "OrderPlaced", 1, clock.instant(),
                UUID.randomUUID(), "proof", lines);
        lines.clear();
        assertThat(event.lines()).containsExactly(new OrderPlacedEvent.Line("SKU-B", 2),
                new OrderPlacedEvent.Line("SKU-A", 1));
        assertThatThrownBy(() -> event.lines().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"with spaces", "injected\r\nheader", "invalid/character"})
    void normalizesMissingOrInvalidCorrelation(String correlation) throws Exception {
        String normalized = mapper.readTree(factory.create(order(), correlation).payload())
                .get("correlationId").asText();
        assertThat(UUID.fromString(normalized).toString()).isEqualTo(normalized);
    }

    @Test
    void normalizesOverlongCorrelation() throws Exception {
        assertThat(mapper.readTree(factory.create(order(), "a".repeat(129)).payload())
                .get("correlationId").asText()).matches("[A-Za-z0-9._:-]{1,128}");
    }

    @Test
    void schemaExampleAndProducedEnvelopeAgreeAndRejectMutations() throws Exception {
        JsonNode schema = resource("order-placed.schema.json");
        assertThat(schema.path("type").asText()).isEqualTo("object");
        assertThat(schema.path("additionalProperties").isBoolean()).isTrue();
        assertThat(schema.path("additionalProperties").booleanValue()).isFalse();
        assertThat(schema.path("required")).isEqualTo(mapper.readTree(
                "[\"eventId\",\"eventType\",\"schemaVersion\",\"occurredAt\",\"orderId\",\"correlationId\",\"lines\"]"));
        JsonNode properties = schema.path("properties");
        var propertyNames = new ArrayList<String>();
        properties.fieldNames().forEachRemaining(propertyNames::add);
        assertThat(propertyNames).containsExactlyInAnyOrder("eventId", "eventType", "schemaVersion",
                "occurredAt", "orderId", "correlationId", "lines");
        for (String field : List.of("eventId", "orderId", "occurredAt", "correlationId", "eventType")) {
            assertThat(properties.path(field).path("type").asText()).isEqualTo("string");
        }
        assertThat(properties.path("eventId").path("format").asText()).isEqualTo("uuid");
        assertThat(properties.path("orderId").path("format").asText()).isEqualTo("uuid");
        assertThat(properties.path("occurredAt").path("format").asText()).isEqualTo("date-time");
        assertThat(properties.path("eventType").path("const").asText()).isEqualTo("OrderPlaced");
        assertThat(properties.path("schemaVersion").path("type").asText()).isEqualTo("integer");
        assertThat(properties.path("schemaVersion").path("const").intValue()).isEqualTo(1);
        assertThat(properties.path("correlationId").path("pattern").asText())
                .isEqualTo("^[A-Za-z0-9._:-]{1,128}$");
        JsonNode lines = properties.path("lines");
        assertThat(lines.path("type").asText()).isEqualTo("array");
        assertThat(lines.path("minItems").intValue()).isEqualTo(1);
        JsonNode item = lines.path("items");
        assertThat(item.path("type").asText()).isEqualTo("object");
        assertThat(item.path("additionalProperties").isBoolean()).isTrue();
        assertThat(item.path("additionalProperties").booleanValue()).isFalse();
        assertThat(item.path("required")).isEqualTo(mapper.readTree("[\"sku\",\"quantity\"]"));
        assertThat(item.path("properties").path("sku").path("type").asText()).isEqualTo("string");
        assertThat(item.path("properties").path("sku").path("minLength").intValue()).isEqualTo(1);
        assertThat(item.path("properties").path("sku").path("maxLength").intValue()).isEqualTo(64);
        assertThat(item.path("properties").path("sku").path("pattern").asText()).isEqualTo("\\S");
        assertThat(item.path("properties").path("quantity").path("type").asText()).isEqualTo("integer");
        assertThat(item.path("properties").path("quantity").path("minimum").intValue()).isEqualTo(1);
        assertThat(item.path("properties").path("quantity").path("maximum").longValue())
                .isEqualTo(Integer.MAX_VALUE);
        for (JsonNode valid : List.of(resource("order-placed.json"),
                mapper.readTree(factory.create(order(), "proof").payload()))) {
            assertContract(valid);
            ObjectNode missingId = valid.deepCopy();
            missingId.remove("eventId");
            assertThatThrownBy(() -> assertContract(missingId)).isInstanceOf(AssertionError.class);
            ObjectNode wrongVersion = valid.deepCopy();
            wrongVersion.put("schemaVersion", 2);
            assertThatThrownBy(() -> assertContract(wrongVersion)).isInstanceOf(AssertionError.class);
            ObjectNode textQuantity = valid.deepCopy();
            ((ObjectNode) textQuantity.path("lines").get(0)).put("quantity", "2");
            assertThatThrownBy(() -> assertContract(textQuantity)).isInstanceOf(AssertionError.class);
        }
    }

    private JsonNode resource(String name) throws Exception {
        try (var input = getClass().getResourceAsStream("/contracts/events/orders/v1/" + name)) {
            assertThat(input).as(name).isNotNull();
            return mapper.readTree(input);
        }
    }

    private void assertContract(JsonNode body) {
        assertThat(body.isObject()).isTrue();
        var fields = new ArrayList<String>();
        body.fieldNames().forEachRemaining(fields::add);
        assertThat(fields).containsExactlyInAnyOrder("eventId", "eventType", "schemaVersion",
                "occurredAt", "orderId", "correlationId", "lines");
        for (String field : List.of("eventId", "orderId", "occurredAt", "correlationId", "eventType")) {
            assertThat(body.path(field).isTextual()).isTrue();
        }
        assertThat(UUID.fromString(body.path("eventId").asText())).isNotNull();
        assertThat(UUID.fromString(body.path("orderId").asText())).isNotNull();
        assertThat(Instant.parse(body.path("occurredAt").asText())).isNotNull();
        assertThat(body.path("eventType").asText()).isEqualTo("OrderPlaced");
        assertThat(body.path("schemaVersion").isIntegralNumber()).isTrue();
        assertThat(body.path("schemaVersion").intValue()).isEqualTo(1);
        assertThat(body.path("correlationId").asText()).matches("[A-Za-z0-9._:-]{1,128}");
        assertThat(body.path("lines").isArray()).isTrue();
        assertThat(body.path("lines").size()).isPositive();
        for (JsonNode line : body.path("lines")) {
            var names = new ArrayList<String>();
            line.fieldNames().forEachRemaining(names::add);
            assertThat(names).containsExactlyInAnyOrder("sku", "quantity");
            assertThat(line.path("sku").isTextual()).isTrue();
            assertThat(line.path("sku").asText()).isNotBlank();
            assertThat(line.path("sku").asText().length()).isBetween(1, 64);
            assertThat(line.path("quantity").isIntegralNumber()).isTrue();
            assertThat(line.path("quantity").longValue()).isBetween(1L, (long) Integer.MAX_VALUE);
        }
    }

    private Order order() {
        Money price = new Money(BigDecimal.TEN, Currency.getInstance("EUR"));
        return Order.place("private-customer", List.of(new OrderLine("SKU-B", 2, price),
                new OrderLine("SKU-A", 1, price)), Instant.parse("2020-01-01T00:00:00Z"));
    }
}
