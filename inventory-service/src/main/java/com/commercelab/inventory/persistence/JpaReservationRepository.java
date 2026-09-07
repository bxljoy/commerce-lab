package com.commercelab.inventory.persistence;

import com.commercelab.inventory.domain.Reservation;
import com.commercelab.inventory.domain.ReservationAlreadyExistsException;
import com.commercelab.inventory.repository.ReservationRepository;
import jakarta.persistence.EntityManager;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class JpaReservationRepository implements ReservationRepository {

    private final ReservationJpaRepository jpa;
    private final EntityManager entityManager;

    public JpaReservationRepository(ReservationJpaRepository jpa, EntityManager entityManager) {
        this.jpa = jpa;
        this.entityManager = entityManager;
    }

    @Override
    @Transactional(readOnly = true)
    public boolean existsByOrderId(UUID orderId) {
        return jpa.existsById(orderId);
    }

    @Override
    @Transactional
    public void add(Reservation reservation) {
        try {
            entityManager.persist(ReservationEntity.fromDomain(reservation));
            entityManager.flush();
        } catch (ConstraintViolationException exception) {
            if ("inventory_reservations_pkey".equals(exception.getConstraintName())) {
                throw new ReservationAlreadyExistsException(reservation.orderId(), exception);
            }
            throw exception;
        }
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Reservation> findByOrderId(UUID orderId) {
        return jpa.findByOrderIdWithLines(orderId).map(ReservationEntity::toDomain);
    }

    @Override
    @Transactional
    public Optional<Reservation> lockByOrderId(UUID orderId) {
        return jpa.lockByOrderId(orderId).map(ReservationEntity::toDomain);
    }

    @Override
    @Transactional
    public void markReleased(UUID orderId, Instant releasedAt) {
        ReservationEntity entity = entityManager.find(ReservationEntity.class, orderId);
        if (entity == null) {
            throw new IllegalStateException("locked reservation is missing: " + orderId);
        }
        entity.markReleased(releasedAt);
    }
}
