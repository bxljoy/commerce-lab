package com.commercelab.order.outbox;

public record OutboxStats(long pendingCount, double oldestPendingAgeSeconds, long failedPendingCount) {}
