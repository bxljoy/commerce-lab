package com.commercelab.inventory.persistence;

import com.commercelab.inventory.domain.Reservation;
import com.commercelab.inventory.domain.ReservationLine;
import com.commercelab.inventory.domain.ReservationStatus;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderBy;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "inventory_reservations")
public class ReservationEntity {

    @Id
    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private ReservationStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "released_at")
    private Instant releasedAt;

    @OneToMany(mappedBy = "reservation", cascade = CascadeType.ALL, orphanRemoval = true)
    @OrderBy("position ASC")
    private List<ReservationLineEntity> lines = new ArrayList<>();

    protected ReservationEntity() {
        // for JPA
    }

    static ReservationEntity fromDomain(Reservation reservation) {
        ReservationEntity entity = new ReservationEntity();
        entity.orderId = reservation.orderId();
        entity.status = reservation.status();
        entity.createdAt = reservation.reservedAt();
        entity.releasedAt = reservation.releasedAt();
        for (int position = 0; position < reservation.lines().size(); position++) {
            ReservationLine line = reservation.lines().get(position);
            entity.lines.add(ReservationLineEntity.fromDomain(line, entity, position));
        }
        return entity;
    }

    Reservation toDomain() {
        return new Reservation(
                orderId,
                lines.stream().map(ReservationLineEntity::toDomain).toList(),
                status,
                createdAt,
                releasedAt);
    }

    UUID orderId() {
        return orderId;
    }

    void markReleased(Instant releaseTime) {
        status = ReservationStatus.RELEASED;
        releasedAt = releaseTime;
    }
}
