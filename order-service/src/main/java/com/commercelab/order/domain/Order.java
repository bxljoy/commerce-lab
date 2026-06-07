package com.commercelab.order.domain;

import java.time.Instant;
import java.util.Currency;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * The Order aggregate root.
 *
 * <p>A record with a compact constructor that enforces its invariants and a defensive
 * copy of {@code lines} (records are only <em>shallowly</em> immutable — without the copy,
 * the caller could keep mutating the list it passed in). All lines share one currency.
 */
public record Order(
        UUID id,
        String customerId,
        OrderStatus status,
        List<OrderLine> lines,
        Instant placedAt
) {

    public Order {
        Objects.requireNonNull(id, "id");
        if (customerId == null || customerId.isBlank()) {
            throw new IllegalArgumentException("customerId is required");
        }
        Objects.requireNonNull(status, "status");
        if (lines == null || lines.isEmpty()) {
            throw new IllegalArgumentException("an order must have at least one line");
        }
        lines = List.copyOf(lines); // defensive copy — see class javadoc
        placedAt = Objects.requireNonNull(placedAt, "placedAt");
    }

    /** Factory for a freshly placed order: server-assigned id, PLACED, stamped now. */
    public static Order place(String customerId, List<OrderLine> lines) {
        return new Order(UUID.randomUUID(), customerId, OrderStatus.PLACED, lines, Instant.now());
    }

    /** The order's currency, taken from its lines (all lines share one currency). */
    public Currency currency() {
        return lines.get(0).unitPrice().currency();
    }

    /** Sum of every line total. */
    public Money total() {
        return lines.stream()
                .map(OrderLine::lineTotal)
                .reduce(Money.zero(currency()), Money::plus);
    }
}
