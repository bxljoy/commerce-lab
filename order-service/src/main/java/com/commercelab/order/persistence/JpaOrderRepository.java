package com.commercelab.order.persistence;

import com.commercelab.order.domain.Order;
import com.commercelab.order.repository.OrderRepository;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * JPA-backed implementation of the domain {@link OrderRepository} port. This is the
 * anti-corruption boundary: it maps the domain {@link Order} record to/from
 * {@link OrderEntity}, so Hibernate entities never escape this class. Reads use the
 * eager-fetching query and map to a fully-materialised record inside the transaction,
 * so the result is safe to use after the session closes (OSIV is off).
 */
@Repository
public class JpaOrderRepository implements OrderRepository {

    private final OrderJpaRepository jpa;

    public JpaOrderRepository(OrderJpaRepository jpa) {
        this.jpa = jpa;
    }

    @Override
    @Transactional
    public Order save(Order order) {
        jpa.save(OrderEntity.fromDomain(order));
        return order;
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Order> findById(UUID id) {
        return jpa.findByIdWithLines(id).map(OrderEntity::toDomain);
    }
}
