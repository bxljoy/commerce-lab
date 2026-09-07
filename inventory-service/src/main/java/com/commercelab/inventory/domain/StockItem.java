package com.commercelab.inventory.domain;

public record StockItem(String sku, int availableQuantity) {

    public StockItem {
        if (sku == null || sku.isBlank()) {
            throw new InvalidReservationException("sku is required");
        }
        if (availableQuantity < 0) {
            throw new InvalidReservationException("availableQuantity cannot be negative");
        }
    }

    public StockItem reserve(int quantity) {
        requirePositive(quantity);
        if (quantity > availableQuantity) {
            throw new InvalidReservationException("cannot reserve more than available stock");
        }
        return new StockItem(sku, availableQuantity - quantity);
    }

    public StockItem release(int quantity) {
        requirePositive(quantity);
        return new StockItem(sku, Math.addExact(availableQuantity, quantity));
    }

    private static void requirePositive(int quantity) {
        if (quantity <= 0) {
            throw new InvalidReservationException("quantity must be positive");
        }
    }
}
