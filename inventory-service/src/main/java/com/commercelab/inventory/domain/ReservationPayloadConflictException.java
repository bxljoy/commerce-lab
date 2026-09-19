package com.commercelab.inventory.domain;

import java.util.UUID;

public class ReservationPayloadConflictException extends RuntimeException {
    public ReservationPayloadConflictException(UUID orderId) {
        super("reservation attempt payload differs for order: " + orderId);
    }
}
