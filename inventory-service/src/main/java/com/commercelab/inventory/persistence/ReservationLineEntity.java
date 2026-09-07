package com.commercelab.inventory.persistence;

import com.commercelab.inventory.domain.ReservationLine;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MapsId;
import jakarta.persistence.Table;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "inventory_reservation_lines")
public class ReservationLineEntity {

    @EmbeddedId
    private ReservationLineId id;

    @MapsId("orderId")
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "order_id", nullable = false)
    private ReservationEntity reservation;

    @Column(nullable = false)
    private int quantity;

    @Column(name = "line_position", nullable = false)
    private int position;

    protected ReservationLineEntity() {
        // for JPA
    }

    static ReservationLineEntity fromDomain(
            ReservationLine line, ReservationEntity reservation, int position) {
        ReservationLineEntity entity = new ReservationLineEntity();
        entity.id = new ReservationLineId(reservation.orderId(), line.sku());
        entity.reservation = reservation;
        entity.quantity = line.quantity();
        entity.position = position;
        return entity;
    }

    ReservationLine toDomain() {
        return new ReservationLine(id.sku, quantity);
    }

    @Embeddable
    public static class ReservationLineId implements Serializable {

        @Column(name = "order_id", nullable = false)
        private UUID orderId;

        @Column(nullable = false, length = 64)
        private String sku;

        protected ReservationLineId() {
            // for JPA
        }

        private ReservationLineId(UUID orderId, String sku) {
            this.orderId = orderId;
            this.sku = sku;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ReservationLineId that)) {
                return false;
            }
            return Objects.equals(orderId, that.orderId) && Objects.equals(sku, that.sku);
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderId, sku);
        }
    }
}
