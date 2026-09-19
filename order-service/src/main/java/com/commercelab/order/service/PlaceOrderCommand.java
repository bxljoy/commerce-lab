package com.commercelab.order.service;

import java.math.BigDecimal;
import java.util.List;

/**
 * Domain-side input for placing an order. The controller maps the generated
 * {@code PlaceOrderRequest} into this, so the service stays free of generated OpenAPI
 * DTOs (the anti-corruption boundary lives in the controller).
 */
public record PlaceOrderCommand(String customerId, String currencyCode, List<Line> lines) {

    public PlaceOrderCommand {
        // Keep null input representable so the creation boundary can return a validation error.
        if (lines != null) lines = java.util.Collections.unmodifiableList(new java.util.ArrayList<>(lines));
    }

    public record Line(String sku, int quantity, BigDecimal unitPrice) {}
}
