package com.commercelab.order.service;

import java.math.BigDecimal;
import java.util.List;

/**
 * Domain-side input for placing an order. The controller maps the generated
 * {@code PlaceOrderRequest} into this, so the service stays free of generated OpenAPI
 * DTOs (the anti-corruption boundary lives in the controller).
 */
public record PlaceOrderCommand(String customerId, String currencyCode, List<Line> lines) {

    public record Line(String sku, int quantity, BigDecimal unitPrice) {}
}
