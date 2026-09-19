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

    public OrderService(OrderRepository repository, OrderCreationService creation) {
        this.repository = repository;
        this.creation = creation;
    }

    /** Pending-only until the reservation coordinator is connected in Task 5. */
    public OrderCreation placeOrder(String key, PlaceOrderCommand command, String correlationId) {
        return creation.createOrReplay(key, command, correlationId);
    }

    /** Retrieve an order or throw {@link OrderNotFoundException} (→ 404). */
    @Transactional(readOnly = true)
    public Order getOrder(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new OrderNotFoundException(id));
    }
}
