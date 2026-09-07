package com.commercelab.inventory.domain;

public record ReservationLine(String sku, int quantity) {

    public ReservationLine {
        if (sku == null || sku.isBlank()) {
            throw new InvalidReservationException("sku is required");
        }
        if (quantity <= 0) {
            throw new InvalidReservationException("quantity must be positive");
        }
    }
}
