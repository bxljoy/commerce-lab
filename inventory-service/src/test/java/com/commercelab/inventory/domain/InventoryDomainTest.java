package com.commercelab.inventory.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class InventoryDomainTest {

    @Test
    void rejectsDuplicateSkus() {
        assertThatThrownBy(() -> Reservation.reserve(UUID.randomUUID(), List.of(
                new ReservationLine("SKU-1", 1),
                new ReservationLine("SKU-1", 2)), Instant.EPOCH))
                .isInstanceOf(InvalidReservationException.class)
                .hasMessageContaining("duplicate SKU: SKU-1");
    }

    @Test
    void preservesRequestOrderAndDefensivelyCopiesLines() {
        ReservationLine second = new ReservationLine("SKU-2", 2);
        ReservationLine first = new ReservationLine("SKU-1", 1);
        List<ReservationLine> requestLines = new ArrayList<>(List.of(second, first));

        Reservation reservation = Reservation.reserve(UUID.randomUUID(), requestLines, Instant.EPOCH);
        requestLines.clear();

        assertThat(reservation.lines()).containsExactly(second, first);
        assertThatThrownBy(() -> reservation.lines().add(new ReservationLine("SKU-3", 3)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsNullOrderId() {
        assertThatThrownBy(() -> Reservation.reserve(null,
                List.of(new ReservationLine("SKU-1", 1)), Instant.EPOCH))
                .isInstanceOf(InvalidReservationException.class);
    }

    @Test
    void rejectsNullReservationTimestamps() {
        UUID orderId = UUID.randomUUID();
        List<ReservationLine> lines = List.of(new ReservationLine("SKU-1", 1));

        assertThatThrownBy(() -> Reservation.reserve(orderId, lines, null))
                .isInstanceOf(InvalidReservationException.class);
        assertThatThrownBy(() -> Reservation.reserve(orderId, lines, Instant.EPOCH).release(null))
                .isInstanceOf(InvalidReservationException.class);
    }

    @Test
    void rejectsEmptyReservationLines() {
        assertThatThrownBy(() -> Reservation.reserve(UUID.randomUUID(), List.of(), Instant.EPOCH))
                .isInstanceOf(InvalidReservationException.class);
    }

    @Test
    void rejectsInvalidReservationStates() {
        UUID orderId = UUID.randomUUID();
        List<ReservationLine> lines = List.of(new ReservationLine("SKU-1", 1));

        assertThatThrownBy(() -> new Reservation(
                orderId, lines, ReservationStatus.RESERVED, Instant.EPOCH, Instant.EPOCH))
                .isInstanceOf(InvalidReservationException.class);
        assertThatThrownBy(() -> new Reservation(
                orderId, lines, ReservationStatus.RELEASED, Instant.EPOCH, null))
                .isInstanceOf(InvalidReservationException.class);
    }

    @Test
    void rejectsBlankReservationLineSku() {
        assertThatThrownBy(() -> new ReservationLine(" ", 1))
                .isInstanceOf(InvalidReservationException.class);
    }

    @Test
    void rejectsNonPositiveReservationLineQuantity() {
        assertThatThrownBy(() -> new ReservationLine("SKU-1", 0))
                .isInstanceOf(InvalidReservationException.class);
        assertThatThrownBy(() -> new ReservationLine("SKU-1", -1))
                .isInstanceOf(InvalidReservationException.class);
    }

    @Test
    void rejectsInvalidStockItem() {
        assertThatThrownBy(() -> new StockItem(" ", 1))
                .isInstanceOf(InvalidReservationException.class);
        assertThatThrownBy(() -> new StockItem("SKU-1", -1))
                .isInstanceOf(InvalidReservationException.class);
    }

    @Test
    void reservesAndRestoresStockWithoutMutation() {
        StockItem original = new StockItem("SKU-1", 2);

        StockItem reserved = original.reserve(2);

        assertThat(original.availableQuantity()).isEqualTo(2);
        assertThat(reserved.availableQuantity()).isZero();
        assertThat(reserved.release(2).availableQuantity()).isEqualTo(2);
    }

    @Test
    void rejectsNonPositiveStockChanges() {
        StockItem stock = new StockItem("SKU-1", 2);

        assertThatThrownBy(() -> stock.reserve(0))
                .isInstanceOf(InvalidReservationException.class);
        assertThatThrownBy(() -> stock.reserve(-1))
                .isInstanceOf(InvalidReservationException.class);
        assertThatThrownBy(() -> stock.release(0))
                .isInstanceOf(InvalidReservationException.class);
        assertThatThrownBy(() -> stock.release(-1))
                .isInstanceOf(InvalidReservationException.class);
    }

    @Test
    void rejectsReservationAboveAvailableStock() {
        assertThatThrownBy(() -> new StockItem("SKU-1", 1).reserve(2))
                .isInstanceOf(InvalidReservationException.class);
    }

    @Test
    void rejectsStockOverflowOnRelease() {
        assertThatThrownBy(() -> new StockItem("SKU-1", Integer.MAX_VALUE).release(1))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void releaseIsAStableTerminalTransition() {
        Reservation reserved = Reservation.reserve(UUID.randomUUID(),
                List.of(new ReservationLine("SKU-1", 1)), Instant.EPOCH);

        Reservation released = reserved.release(Instant.EPOCH.plusSeconds(1));

        assertThat(reserved.status()).isEqualTo(ReservationStatus.RESERVED);
        assertThat(reserved.releasedAt()).isNull();
        assertThat(released.status()).isEqualTo(ReservationStatus.RELEASED);
        assertThat(released.releasedAt()).isEqualTo(Instant.EPOCH.plusSeconds(1));
        assertThat(released.release(Instant.EPOCH.plusSeconds(2))).isEqualTo(released);
    }
}
