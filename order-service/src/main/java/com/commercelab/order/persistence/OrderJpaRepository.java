package com.commercelab.order.persistence;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Spring Data repository over {@link OrderEntity}. Package-private — only the
 * {@link JpaOrderRepository} adapter uses it; the rest of the app depends on the domain
 * {@code OrderRepository} port.
 *
 * <p>The inherited {@code findById} leaves {@code lines} LAZY (used to demonstrate the
 * OSIV-off lazy-init behaviour in tests). {@link #findByIdWithLines} opts into an eager
 * join-fetch via {@code @EntityGraph} for the real read path — one query, no N+1, and the
 * returned entity is fully populated before the session closes.
 */
interface OrderJpaRepository extends JpaRepository<OrderEntity, UUID> {

    @EntityGraph(attributePaths = "lines")
    @Query("select o from OrderEntity o where o.id = :id")
    Optional<OrderEntity> findByIdWithLines(@Param("id") UUID id);
}
