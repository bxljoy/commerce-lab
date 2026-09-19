package com.commercelab.order.outbox;

import java.util.UUID;

/** The serialized publication intent. Retries must send payload unchanged. */
public record OutboxMessage(UUID eventId, UUID orderId, String topic, String messageKey, String payload) {}
