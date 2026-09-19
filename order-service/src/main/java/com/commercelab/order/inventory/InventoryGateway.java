package com.commercelab.order.inventory;

import java.util.UUID;

public interface InventoryGateway {
    InventoryOutcome reserve(InventoryRequest request, String correlationId);
    InventoryOutcome find(UUID orderId, String correlationId);
}
