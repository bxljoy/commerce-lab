package com.commercelab.inventory.domain;

import java.util.UUID;

public class ReservationNotFoundException extends RuntimeException {

    public ReservationNotFoundException(UUID orderId) {
        super("reservation not found for order: " + orderId);
    }
}
