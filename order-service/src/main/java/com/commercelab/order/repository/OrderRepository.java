package com.commercelab.order.repository;

import com.commercelab.order.domain.Order;
import java.util.Optional;
import java.util.UUID;

/**
 * Persistence boundary for orders. Phase 1 backs this with an in-memory map; Phase 2
 * swaps in a JPA implementation behind the same interface, leaving service/controller
 * untouched.
 */
public interface OrderRepository {

    /** Insert a newly placed order. Updating an existing aggregate is a separate operation. */
    Order add(Order order);

    Optional<Order> findById(UUID id);
}
