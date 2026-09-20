package com.commercelab.inventory.outbox;

@FunctionalInterface
public interface OutboxPublisher {
    void publish(OutboxMessage message) throws InterruptedException;
}
