package com.commercelab.order.outbox;

@FunctionalInterface
public interface OutboxPublicationHook {
    void afterAcknowledgement(OutboxMessage message) throws InterruptedException;
}
