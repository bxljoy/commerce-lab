package com.commercelab.inventory.domain;

import java.util.UUID;

public class ReservationAlreadyExistsException extends RuntimeException {

    public ReservationAlreadyExistsException(UUID orderId) {
        super("reservation already exists for order: " + orderId);
    }

    public ReservationAlreadyExistsException(UUID orderId, Throwable cause) {
        super("reservation already exists for order: " + orderId, cause);
    }
}
