package com.commercelab.inventory.domain;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public record Reservation(
        UUID orderId,
        List<ReservationLine> lines,
        ReservationStatus status,
        Instant reservedAt,
        Instant releasedAt
) {

    public Reservation {
        if (orderId == null) {
            throw new InvalidReservationException("orderId is required");
        }
        if (lines == null || lines.isEmpty()) {
            throw new InvalidReservationException("reservation must have at least one line");
        }
        if (status == null) {
            throw new InvalidReservationException("status is required");
        }
        if (reservedAt == null) {
            throw new InvalidReservationException("reservedAt is required");
        }
        if (status == ReservationStatus.RESERVED && releasedAt != null) {
            throw new InvalidReservationException("reserved reservation cannot have releasedAt");
        }
        if (status == ReservationStatus.RELEASED && releasedAt == null) {
            throw new InvalidReservationException("released reservation requires releasedAt");
        }
        Set<String> skus = new HashSet<>();
        for (ReservationLine line : lines) {
            if (!skus.add(line.sku())) {
                throw new InvalidReservationException("duplicate SKU: " + line.sku());
            }
        }
        lines = List.copyOf(lines);
    }

    public static Reservation reserve(UUID orderId, List<ReservationLine> lines, Instant reservedAt) {
        return new Reservation(orderId, lines, ReservationStatus.RESERVED, reservedAt, null);
    }

    public Reservation release(Instant releasedAt) {
        if (releasedAt == null) {
            throw new InvalidReservationException("releasedAt is required");
        }
        if (status == ReservationStatus.RELEASED) {
            return this;
        }
        return new Reservation(orderId, lines, ReservationStatus.RELEASED, reservedAt, releasedAt);
    }
}
