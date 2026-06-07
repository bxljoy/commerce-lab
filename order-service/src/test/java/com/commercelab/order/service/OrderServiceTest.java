package com.commercelab.order.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.commercelab.order.domain.Order;
import com.commercelab.order.domain.OrderNotFoundException;
import com.commercelab.order.domain.OrderStatus;
import com.commercelab.order.repository.InMemoryOrderRepository;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Service unit tests against the real in-memory repository (fast, no Spring context, no
 * mocks needed). The HTTP boundary is covered separately by the @WebMvcTest slice.
 */
class OrderServiceTest {

    private OrderService service;

    @BeforeEach
    void setUp() {
        service = new OrderService(new InMemoryOrderRepository());
    }

    @Test
    void placeOrderPersistsPlacedOrderWithComputedTotal() {
        PlaceOrderCommand command = new PlaceOrderCommand("cust-1", "EUR", List.of(
                new PlaceOrderCommand.Line("SKU-1", 2, new BigDecimal("9.99"))));

        Order placed = service.placeOrder(command);

        assertThat(placed.status()).isEqualTo(OrderStatus.PLACED);
        assertThat(placed.total().amount()).isEqualByComparingTo("19.98");
        assertThat(service.getOrder(placed.id())).isEqualTo(placed);
    }

    @Test
    void getOrderThrowsWhenMissing() {
        assertThatThrownBy(() -> service.getOrder(UUID.randomUUID()))
                .isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void placeOrderRejectsUnknownCurrencyCode() {
        PlaceOrderCommand command = new PlaceOrderCommand("cust-1", "ZZZ", List.of(
                new PlaceOrderCommand.Line("SKU-1", 1, BigDecimal.ONE)));

        assertThatThrownBy(() -> service.placeOrder(command))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
