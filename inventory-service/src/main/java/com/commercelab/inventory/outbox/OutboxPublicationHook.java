package com.commercelab.inventory.outbox;

@FunctionalInterface
public interface OutboxPublicationHook {
    void afterAcknowledgement(OutboxMessage message) throws InterruptedException;
}
