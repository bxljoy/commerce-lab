package com.commercelab.order.domain;

/** Current lifecycle plus retained historical states. */
public enum OrderStatus {
    PENDING_INVENTORY,
    REJECTED,
    PLACED,
    CONFIRMED,
    SHIPPED,
    CANCELLED
}
