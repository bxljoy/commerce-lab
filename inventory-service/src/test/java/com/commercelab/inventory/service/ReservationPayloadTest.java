package com.commercelab.inventory.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.commercelab.inventory.domain.InvalidReservationException;
import com.commercelab.inventory.domain.ReservationLine;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ReservationPayloadTest {

    @Test
    void comparesSkuQuantityContentRegardlessOfOrderOrOrderId() {
        var payload = payload(new ReserveInventoryCommand.Line("B", 2),
                new ReserveInventoryCommand.Line("A", 1));
        var reordered = payload(new ReserveInventoryCommand.Line("A", 1),
                new ReserveInventoryCommand.Line("B", 2));

        assertThat(payload).isEqualTo(reordered);
        assertThat(payload.hashCode()).isEqualTo(reordered.hashCode());
        assertThat(payload.lines()).containsExactly(
                new ReservationLine("A", 1), new ReservationLine("B", 2));
    }

    @Test
    void distinguishesChangedQuantityAndSku() {
        var original = payload(new ReserveInventoryCommand.Line("A", 1));

        assertThat(original).isNotEqualTo(payload(new ReserveInventoryCommand.Line("A", 2)))
                .isNotEqualTo(payload(new ReserveInventoryCommand.Line("B", 1)));
    }

    @Test
    void producesSkuSortedJsonWithOnlySkuAndQuantity() throws Exception {
        var payload = payload(new ReserveInventoryCommand.Line("B", 2),
                new ReserveInventoryCommand.Line("A", 1));

        assertThat(payload.canonicalJson()).isEqualTo(new ObjectMapper().readTree("""
                [{"sku":"A","quantity":1},{"sku":"B","quantity":2}]
                """));
    }

    @ParameterizedTest
    @MethodSource("invalidLines")
    void rejectsMalformedLinesBeforePersistence(List<ReserveInventoryCommand.Line> lines) {
        assertThatThrownBy(() -> ReservationPayload.from(
                new ReserveInventoryCommand(UUID.randomUUID(), lines)))
                .isInstanceOf(InvalidReservationException.class);
    }

    static Stream<List<ReserveInventoryCommand.Line>> invalidLines() {
        return Stream.of(
                List.of(new ReserveInventoryCommand.Line("A", 1), new ReserveInventoryCommand.Line("A", 2)),
                Arrays.asList((ReserveInventoryCommand.Line) null),
                List.of(new ReserveInventoryCommand.Line(" ", 1)),
                List.of(new ReserveInventoryCommand.Line(null, 1)),
                List.of(new ReserveInventoryCommand.Line("A", 0)),
                List.of(new ReserveInventoryCommand.Line("A", -1)),
                List.of());
    }

    @Test
    void retainsCanonicalContentAfterCallerListMutation() throws Exception {
        var lines = new ArrayList<>(List.of(new ReserveInventoryCommand.Line("A", 1)));
        var payload = ReservationPayload.from(new ReserveInventoryCommand(UUID.randomUUID(), lines));

        lines.clear();
        lines.add(new ReserveInventoryCommand.Line("B", 2));

        assertThat(payload.canonicalJson()).isEqualTo(new ObjectMapper().readTree("""
                [{"sku":"A","quantity":1}]
                """));
        assertThatThrownBy(() -> payload.lines().add(new ReservationLine("B", 2)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void returnedJsonCannotMutatePayloadIdentity() throws Exception {
        var payload = payload(new ReserveInventoryCommand.Line("A", 1));
        var originalHash = payload.hashCode();

        ((ObjectNode) payload.canonicalJson().get(0)).put("quantity", 99);

        assertThat(payload.canonicalJson()).isEqualTo(new ObjectMapper().readTree("""
                [{"sku":"A","quantity":1}]
                """));
        assertThat(payload).isEqualTo(payload(new ReserveInventoryCommand.Line("A", 1)));
        assertThat(payload.hashCode()).isEqualTo(originalHash);
    }

    private static ReservationPayload payload(ReserveInventoryCommand.Line... lines) {
        return ReservationPayload.from(new ReserveInventoryCommand(UUID.randomUUID(), List.of(lines)));
    }
}
