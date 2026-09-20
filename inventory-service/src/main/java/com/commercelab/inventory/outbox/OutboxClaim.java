package com.commercelab.inventory.outbox;

import java.util.UUID;

public record OutboxClaim(OutboxMessage message, UUID token, long attemptCount) {}
