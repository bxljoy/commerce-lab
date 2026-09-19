package com.commercelab.order.service;

import com.commercelab.order.domain.Order;

public record PendingOrderSnapshot(Order order, int attemptCount, String correlationId) {}
