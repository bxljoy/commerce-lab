package com.commercelab.order.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

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
 * Read-service unit tests against the real in-memory repository. Atomic creation
 * is covered against PostgreSQL in OrderIdempotencyIT.
 */
class OrderServiceTest {

    private OrderService service;
    private InMemoryOrderRepository repository;
    private OrderCreationService creation;

    @BeforeEach
    void setUp() {
        repository = new InMemoryOrderRepository();
        creation = mock(OrderCreationService.class);
        service = new OrderService(repository, creation);
    }

    @Test
    void placeOrderReturnsDurablyCreatedPendingOrderUnchanged() {
        var command = new PlaceOrderCommand("cust", "EUR", List.of(
                new PlaceOrderCommand.Line("A", 1, BigDecimal.ONE)));
        var payload = OrderPayload.from(command);
        var result = new OrderCreation(Order.place(payload.customerId(), payload.lines()), true);
        org.mockito.Mockito.when(creation.createOrReplay("key", command, "correlation")).thenReturn(result);
        assertThat(service.placeOrder("key", command, "correlation")).isSameAs(result);
    }

    @Test
    void getOrderReturnsPersistedPendingOrderWithComputedTotal() {
        PlaceOrderCommand command = new PlaceOrderCommand("cust-1", "EUR", List.of(
                new PlaceOrderCommand.Line("SKU-1", 2, new BigDecimal("9.99"))));

        var payload = OrderPayload.from(command);
        Order placed = repository.add(Order.place(payload.customerId(), payload.lines()));

        assertThat(placed.status()).isEqualTo(OrderStatus.PENDING_INVENTORY);
        assertThat(placed.total().amount()).isEqualByComparingTo("19.98");
        assertThat(service.getOrder(placed.id())).isEqualTo(placed);
    }

    @Test
    void getOrderThrowsWhenMissing() {
        assertThatThrownBy(() -> service.getOrder(UUID.randomUUID()))
                .isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void creationPayloadRejectsUnknownCurrencyCode() {
        PlaceOrderCommand command = new PlaceOrderCommand("cust-1", "ZZZ", List.of(
                new PlaceOrderCommand.Line("SKU-1", 1, BigDecimal.ONE)));

        assertThatThrownBy(() -> OrderPayload.from(command))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
