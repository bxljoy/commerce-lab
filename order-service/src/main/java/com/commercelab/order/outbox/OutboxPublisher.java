package com.commercelab.order.outbox;

@FunctionalInterface
public interface OutboxPublisher {
    void publish(OutboxMessage message) throws InterruptedException;
}
