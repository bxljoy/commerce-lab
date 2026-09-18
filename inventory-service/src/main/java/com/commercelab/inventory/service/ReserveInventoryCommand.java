package com.commercelab.inventory.service;

import com.commercelab.inventory.domain.InvalidReservationException;
import com.commercelab.inventory.domain.ReservationLine;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record ReserveInventoryCommand(UUID orderId, List<Line> lines) {

    public ReserveInventoryCommand {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(lines, "lines");
        if (lines.stream().anyMatch(Objects::isNull)) {
            throw new InvalidReservationException("reservation line is required");
        }
        lines = List.copyOf(lines);
    }

    public List<ReservationLine> toDomainLines() {
        return lines.stream()
                .map(line -> new ReservationLine(line.sku(), line.quantity()))
                .toList();
    }

    public record Line(String sku, int quantity) {}
}
