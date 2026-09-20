package com.commercelab.inventory.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("inventory.outbox")
public record OutboxProperties(
        @DefaultValue("1000") long pollIntervalMs,
        @DefaultValue("20") int maxAttemptsPerPass,
        @DefaultValue("60000") long leaseMs,
        @DefaultValue("12000") long ackWaitMs,
        @DefaultValue("2000") long maxBlockMs,
        @DefaultValue("10000") int deliveryTimeoutMs,
        @DefaultValue("3000") int requestTimeoutMs) {

    public OutboxProperties {
        if (pollIntervalMs <= 0 || maxAttemptsPerPass <= 0 || leaseMs <= 0 || ackWaitMs <= 0
                || maxBlockMs <= 0 || deliveryTimeoutMs <= 0 || requestTimeoutMs <= 0) {
            throw new IllegalArgumentException("Outbox budgets must be positive");
        }
        // Subtraction avoids overflow when validating externally supplied long values.
        if (ackWaitMs <= deliveryTimeoutMs || requestTimeoutMs > deliveryTimeoutMs
                || leaseMs <= ackWaitMs || leaseMs - ackWaitMs <= maxBlockMs) {
            throw new IllegalArgumentException("Outbox requires request <= delivery < acknowledgement and lease > block + acknowledgement");
        }
    }

    public static OutboxProperties defaults() {
        return new OutboxProperties(1000, 20, 60000, 12000, 2000, 10000, 3000);
    }
}
