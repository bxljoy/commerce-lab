package com.commercelab.inventory.service;

import com.commercelab.inventory.domain.InvalidReservationException;
import com.commercelab.inventory.domain.ReservationLine;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record ReservationPayload(List<ReservationLine> lines) {

    public ReservationPayload {
        if (lines == null || lines.isEmpty()) {
            throw new InvalidReservationException("reservation must have at least one line");
        }
        Set<String> skus = new HashSet<>();
        for (ReservationLine line : lines) {
            if (line == null) {
                throw new InvalidReservationException("reservation line is required");
            }
            if (!skus.add(line.sku())) {
                throw new InvalidReservationException("duplicate SKU: " + line.sku());
            }
        }
        lines = lines.stream().sorted(Comparator.comparing(ReservationLine::sku)).toList();
    }

    public static ReservationPayload from(ReserveInventoryCommand command) {
        return new ReservationPayload(command.toDomainLines());
    }

    public JsonNode canonicalJson() {
        ArrayNode result = JsonNodeFactory.instance.arrayNode();
        for (ReservationLine line : lines) {
            result.addObject().put("sku", line.sku()).put("quantity", line.quantity());
        }
        return result;
    }
}
