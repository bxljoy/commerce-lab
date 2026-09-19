package com.commercelab.order.inventory;

import java.util.List;
import java.util.UUID;

public record InventoryRequest(UUID orderId, List<InventoryLine> lines) {
    public InventoryRequest { lines = List.copyOf(lines); }
}
