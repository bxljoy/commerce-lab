package com.commercelab.inventory.persistence;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

interface ReservationJpaRepository extends JpaRepository<ReservationEntity, UUID> {

    @EntityGraph(attributePaths = "lines")
    @Query("select r from ReservationEntity r where r.orderId = :orderId")
    Optional<ReservationEntity> findByOrderIdWithLines(@Param("orderId") UUID orderId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @EntityGraph(attributePaths = "lines")
    @Query("select r from ReservationEntity r where r.orderId = :orderId")
    Optional<ReservationEntity> lockByOrderId(@Param("orderId") UUID orderId);
}
