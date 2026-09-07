package com.commercelab.order.domain;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * A single line of an order: a SKU, a quantity, and the unit price (as {@link Money}).
 */
public record OrderLine(String sku, int quantity, Money unitPrice) {

    public static final int MAX_UNIT_PRICE_SCALE = 4;
    public static final BigDecimal MAX_UNIT_PRICE = new BigDecimal("999999999999999.9999");

    public OrderLine {
        if (sku == null || sku.isBlank()) {
            throw new IllegalArgumentException("sku is required");
        }
        if (quantity < 1) {
            throw new IllegalArgumentException("quantity must be >= 1, was " + quantity);
        }
        Objects.requireNonNull(unitPrice, "unitPrice");
        BigDecimal amount = unitPrice.amount().stripTrailingZeros();
        if (amount.signum() <= 0) {
            throw new IllegalArgumentException("unitPrice must be > 0, was " + unitPrice.amount());
        }
        if (Math.max(amount.scale(), 0) > MAX_UNIT_PRICE_SCALE) {
            throw new IllegalArgumentException(
                    "unitPrice supports at most " + MAX_UNIT_PRICE_SCALE + " fractional digits, was "
                            + unitPrice.amount());
        }
        if (amount.compareTo(MAX_UNIT_PRICE) > 0) {
            throw new IllegalArgumentException("unitPrice exceeds supported maximum " + MAX_UNIT_PRICE);
        }
    }

    /** Extended price for this line: unit price × quantity. */
    public Money lineTotal() {
        return unitPrice.times(quantity);
    }
}
