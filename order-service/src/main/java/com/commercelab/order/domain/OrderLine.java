package com.commercelab.order.domain;

import java.util.Objects;

/**
 * A single line of an order: a SKU, a quantity, and the unit price (as {@link Money}).
 */
public record OrderLine(String sku, int quantity, Money unitPrice) {

    public OrderLine {
        if (sku == null || sku.isBlank()) {
            throw new IllegalArgumentException("sku is required");
        }
        if (quantity < 1) {
            throw new IllegalArgumentException("quantity must be >= 1, was " + quantity);
        }
        Objects.requireNonNull(unitPrice, "unitPrice");
    }

    /** Extended price for this line: unit price × quantity. */
    public Money lineTotal() {
        return unitPrice.times(quantity);
    }
}
