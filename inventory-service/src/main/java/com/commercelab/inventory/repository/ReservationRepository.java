package com.commercelab.inventory.repository;

import com.commercelab.inventory.domain.Reservation;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface ReservationRepository {

    boolean existsByOrderId(UUID orderId);

    void add(Reservation reservation);

    Optional<Reservation> findByOrderId(UUID orderId);

    Optional<Reservation> lockByOrderId(UUID orderId);

    void markReleased(UUID orderId, Instant releasedAt);
}
