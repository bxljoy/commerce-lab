package com.commercelab.order.persistence;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Spring Data repository used by the creation adapter and managed progress updates.
 *
 * <p>The inherited {@code findById} leaves {@code lines} LAZY (used to demonstrate the
 * OSIV-off lazy-init behaviour in tests). {@link #findByIdWithLines} opts into an eager
 * join-fetch via {@code @EntityGraph} for the real read path — one query, no N+1, and the
 * returned entity is fully populated before the session closes.
 */
public interface OrderJpaRepository extends JpaRepository<OrderEntity, UUID> {

    @EntityGraph(attributePaths = "lines")
    @Query("select o from OrderEntity o where o.id = :id")
    Optional<OrderEntity> findByIdWithLines(@Param("id") UUID id);

    @Query("""
            select o.id from OrderEntity o
            where o.status = com.commercelab.order.domain.OrderStatus.PENDING_INVENTORY
              and o.recoveryBlocked = false and o.nextAttemptAt <= :now
            order by o.nextAttemptAt, o.id
            """)
    List<UUID> findDueIds(@Param("now") Instant now, Pageable pageable);
}
