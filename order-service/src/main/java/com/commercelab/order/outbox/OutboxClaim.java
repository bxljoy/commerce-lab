package com.commercelab.order.outbox;

import java.util.UUID;

public record OutboxClaim(OutboxMessage message, UUID token, long attemptCount) {}
