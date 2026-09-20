package com.commercelab.inventory.outbox;

public record OutboxStats(long pendingCount, double oldestPendingAgeSeconds, long failedPendingCount) {}
