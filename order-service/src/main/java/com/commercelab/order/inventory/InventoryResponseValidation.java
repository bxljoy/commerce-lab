package com.commercelab.order.inventory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class InventoryResponseValidation {
    private InventoryResponseValidation() {}

    public static void requireMatch(InventoryRequest expected, UUID orderId, List<InventoryLine> lines) {
        if (!expected.orderId().equals(orderId) || !lineMap(expected.lines()).equals(lineMap(lines))) {
            throw new InventoryProtocolException("INVENTORY_RESPONSE_MISMATCH");
        }
    }

    static Map<String, Integer> lineMap(List<InventoryLine> lines) {
        var result = new HashMap<String, Integer>();
        if (lines == null || lines.isEmpty()) throw new InventoryProtocolException("INVENTORY_INVALID_RESPONSE");
        for (var line : lines) {
            if (line == null || line.sku() == null || line.sku().isBlank() || line.sku().length() > 64
                    || line.quantity() < 1 || result.putIfAbsent(line.sku(), line.quantity()) != null) {
                throw new InventoryProtocolException("INVENTORY_INVALID_RESPONSE");
            }
        }
        return result;
    }
}
