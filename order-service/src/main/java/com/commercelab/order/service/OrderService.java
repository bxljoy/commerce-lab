package com.commercelab.order.service;

import com.commercelab.order.domain.Order;
import com.commercelab.order.domain.OrderNotFoundException;
import com.commercelab.order.repository.OrderRepository;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Order application service — orchestrates the domain and the repository. Works purely
 * in domain types ({@link PlaceOrderCommand} in, {@link Order} out); DTO mapping is the
 * controller's job.
 */
@Service
public class OrderService {

    private final OrderRepository repository;
    private final OrderCreationService creation;
    private final OrderReservationCoordinator reservations;

    public OrderService(OrderRepository repository, OrderCreationService creation, OrderReservationCoordinator reservations) {
        this.repository = repository;
        this.creation = creation;
        this.reservations = reservations;
    }

    public OrderCreation placeOrder(String key, PlaceOrderCommand command, String correlationId) {
        OrderCreation result = creation.createOrReplay(key, command, correlationId);
        return result.created() ? new OrderCreation(reservations.attempt(result.order().id()), true) : result;
    }

    /** Retrieve an order or throw {@link OrderNotFoundException} (→ 404). */
    @Transactional(readOnly = true)
    public Order getOrder(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new OrderNotFoundException(id));
    }
}
