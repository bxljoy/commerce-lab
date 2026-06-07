package com.commercelab.order.domain;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.Objects;

/**
 * Money value object — an amount in a single ISO-4217 currency.
 *
 * <p>The natural Phase 1 use of records: an immutable value object with value-based
 * equality and invariants enforced in the compact constructor. (The sealed-hierarchy +
 * exhaustive-switch half of the records note lands in Phase 4 with the event-driven
 * order state machine.)
 */
public record Money(BigDecimal amount, Currency currency) {

    public Money {
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(currency, "currency");
        if (amount.signum() < 0) {
            throw new IllegalArgumentException("amount must be >= 0, was " + amount);
        }
    }

    public static Money zero(Currency currency) {
        return new Money(BigDecimal.ZERO, currency);
    }

    public Money plus(Money other) {
        requireSameCurrency(other);
        return new Money(amount.add(other.amount), currency);
    }

    public Money times(int quantity) {
        if (quantity < 0) {
            throw new IllegalArgumentException("quantity must be >= 0, was " + quantity);
        }
        return new Money(amount.multiply(BigDecimal.valueOf(quantity)), currency);
    }

    private void requireSameCurrency(Money other) {
        if (!currency.equals(other.currency)) {
            throw new IllegalArgumentException("currency mismatch: %s vs %s"
                    .formatted(currency.getCurrencyCode(), other.currency.getCurrencyCode()));
        }
    }
}
