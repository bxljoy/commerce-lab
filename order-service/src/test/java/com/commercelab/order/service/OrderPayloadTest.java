package com.commercelab.order.service;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class OrderPayloadTest {
    private static PlaceOrderCommand.Line line(String sku, String price) {
        return new PlaceOrderCommand.Line(sku, 2, new BigDecimal(price));
    }

    @Test
    void numericScaleIsEquivalentButLineOrderAndIdentifiersAreExact() {
        var original = new PlaceOrderCommand("cust", "EUR", List.of(line("B", "9.9900"), line("A", "10.00")));
        var payload = OrderPayload.from(original);
        assertThat(payload.canonicalJson()).isEqualTo(OrderPayload.from(new PlaceOrderCommand(
                "cust", "EUR", List.of(line("B", "9.99"), line("A", "10")))).canonicalJson());
        assertThat(payload.canonicalJson()).isNotEqualTo(OrderPayload.from(new PlaceOrderCommand(
                "cust", "EUR", original.lines().reversed())).canonicalJson());
        assertThat(payload.canonicalJson()).isNotEqualTo(OrderPayload.from(new PlaceOrderCommand(
                "cust ", "EUR", original.lines())).canonicalJson());
        assertThat(payload.canonicalJson()).isNotEqualTo(OrderPayload.from(new PlaceOrderCommand(
                "cust", "USD", original.lines())).canonicalJson());
    }

    @Test
    void payloadAndJsonAreDefensiveSnapshots() {
        var lines = new ArrayList<>(List.of(line("A", "1")));
        var payload = OrderPayload.from(new PlaceOrderCommand("cust", "EUR", lines));
        var before = payload.canonicalJson();
        lines.clear();
        ((com.fasterxml.jackson.databind.node.ObjectNode) payload.canonicalJson()).removeAll();
        assertThat(payload.canonicalJson()).isEqualTo(before);
        assertThat(payload.lines()).hasSize(1);
        assertThatThrownBy(() -> payload.lines().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void invalidCommandsAreRejected() {
        for (var command : List.of(
                new PlaceOrderCommand("cust", "EUR", Arrays.asList((PlaceOrderCommand.Line) null)),
                new PlaceOrderCommand("cust", "EUR", List.of(line("A", "1"), line("A", "2"))),
                new PlaceOrderCommand("cust", "eur", List.of(line("A", "1"))),
                new PlaceOrderCommand("cust", "ZZZ", List.of(line("A", "1"))),
                new PlaceOrderCommand("cust", "EUR", null),
                new PlaceOrderCommand(" ", "EUR", List.of(line("A", "1"))),
                new PlaceOrderCommand("x".repeat(65), "EUR", List.of(line("A", "1"))),
                new PlaceOrderCommand("cust", "EUR", List.of(line("x".repeat(65), "1"))),
                new PlaceOrderCommand("cust", "EUR", List.of(line("A", "0.00001"))),
                new PlaceOrderCommand("cust", "EUR", List.of(line("A", "1000000000000000"))),
                new PlaceOrderCommand("cust", "EUR", List.of(new PlaceOrderCommand.Line("A", 0, BigDecimal.ONE))),
                new PlaceOrderCommand("cust", "EUR", List.of(new PlaceOrderCommand.Line("A", 1, null))))) {
            assertThatThrownBy(() -> OrderPayload.from(command)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> OrderPayload.from(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
