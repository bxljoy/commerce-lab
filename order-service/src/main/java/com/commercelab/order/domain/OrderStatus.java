package com.commercelab.order.domain;

/**
 * Order lifecycle states. Phase 1 only ever produces {@link #PLACED}; the transitions
 * to CONFIRMED / SHIPPED / CANCELLED are driven by inventory events in Phase 3–4, where
 * a sealed state model + exhaustive switch will enforce legal transitions at compile time.
 */
public enum OrderStatus {
    PLACED,
    CONFIRMED,
    SHIPPED,
    CANCELLED
}
