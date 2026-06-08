package com.commercelab.order.repository;

import com.commercelab.order.domain.Order;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link OrderRepository} fake for fast, Spring-free unit tests
 * (e.g. {@code OrderServiceTest}). Production uses the JPA-backed adapter; this lives in
 * test sources only so it is never wired as a bean.
 */
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
