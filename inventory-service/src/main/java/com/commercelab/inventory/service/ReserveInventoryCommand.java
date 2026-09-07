package com.commercelab.inventory.service;

import com.commercelab.inventory.domain.ReservationLine;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record ReserveInventoryCommand(UUID orderId, List<Line> lines) {

    public ReserveInventoryCommand {
        Objects.requireNonNull(orderId, "orderId");
        lines = List.copyOf(Objects.requireNonNull(lines, "lines"));
    }

    public List<ReservationLine> toDomainLines() {
        return lines.stream()
                .map(line -> new ReservationLine(line.sku(), line.quantity()))
                .toList();
    }

    public record Line(String sku, int quantity) {}
}
