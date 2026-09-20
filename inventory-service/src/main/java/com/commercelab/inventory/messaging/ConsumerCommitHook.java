package com.commercelab.inventory.messaging;

import java.util.UUID;

/** Runs after the application transaction commits and before the listener returns. */
@FunctionalInterface
public interface ConsumerCommitHook {
    void afterDatabaseCommit(UUID eventId, ProcessingOutcome outcome);
}
