package com.commercelab.order.inventory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public sealed interface InventoryOutcome {
    record Reserved(UUID orderId, List<InventoryLine> lines) implements InventoryOutcome {
        public Reserved { lines = List.copyOf(lines); }
    }
    record Released(UUID orderId, List<InventoryLine> lines) implements InventoryOutcome {
        public Released { lines = List.copyOf(lines); }
    }
    record Rejected(Map<String, StockShortage> unavailable) implements InventoryOutcome {
        public Rejected { unavailable = Map.copyOf(unavailable); }
    }
    record Missing() implements InventoryOutcome {}
}
