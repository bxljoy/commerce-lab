package com.commercelab.order.repository;

import com.commercelab.order.domain.Order;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Repository;

/**
 * In-memory {@link OrderRepository} for Phase 1 — a thread-safe map, no persistence.
 * Orders do not survive a restart; Phase 2 replaces this with Postgres + JPA.
 */
@Repository
public class InMemoryOrderRepository implements OrderRepository {

    private final Map<UUID, Order> store = new ConcurrentHashMap<>();

    @Override
    public Order save(Order order) {
        store.put(order.id(), order);
        return order;
    }

    @Override
    public Optional<Order> findById(UUID id) {
        return Optional.ofNullable(store.get(id));
    }
}
