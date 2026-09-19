package com.commercelab.order.service;

import com.commercelab.order.domain.Order;

public record OrderCreation(Order order, boolean created) {}
