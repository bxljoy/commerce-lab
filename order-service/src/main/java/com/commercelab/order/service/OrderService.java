package com.commercelab.order.service;

import com.commercelab.order.domain.Money;
import com.commercelab.order.domain.Order;
import com.commercelab.order.domain.OrderLine;
import com.commercelab.order.domain.OrderNotFoundException;
import com.commercelab.order.repository.OrderRepository;
import java.util.Currency;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * Order application service — orchestrates the domain and the repository. Works purely
 * in domain types ({@link PlaceOrderCommand} in, {@link Order} out); DTO mapping is the
 * controller's job.
 */
@Service
public class OrderService {

    private final OrderRepository repository;

    public OrderService(OrderRepository repository) {
        this.repository = repository;
    }

    /** Build the order aggregate, persist it, and return it (status PLACED). */
    public Order placeOrder(PlaceOrderCommand command) {
        Currency currency = Currency.getInstance(command.currencyCode()); // IllegalArgumentException on bad code → 400
        List<OrderLine> lines = command.lines().stream()
                .map(line -> new OrderLine(line.sku(), line.quantity(), new Money(line.unitPrice(), currency)))
                .toList();
        return repository.save(Order.place(command.customerId(), lines));
    }

    /** Retrieve an order or throw {@link OrderNotFoundException} (→ 404). */
    public Order getOrder(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new OrderNotFoundException(id));
    }
}
