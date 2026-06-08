package com.commercelab.order.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.commercelab.order.AbstractPostgresIntegrationTest;
import com.commercelab.order.domain.Money;
import com.commercelab.order.domain.Order;
import com.commercelab.order.domain.OrderLine;
import com.commercelab.order.domain.OrderStatus;
import java.math.BigDecimal;
import java.util.Currency;
import java.util.List;
import org.hibernate.LazyInitializationException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Testcontainers-backed persistence integration test (real Postgres, Flyway-migrated,
 * Hibernate validating). Proves the order round-trips through the database, that the
 * adapter's explicit fetch makes the result safe to use after the session closes, and —
 * the OSIV-off proof — that a naive lazy load throws once its short-lived session ends.
 */
@SpringBootTest
class OrderPersistenceIT extends AbstractPostgresIntegrationTest {

    private static final Currency EUR = Currency.getInstance("EUR");

    @Autowired
    private JpaOrderRepository adapter;   // domain port (maps entity <-> domain record)

    @Autowired
    private OrderJpaRepository jpa;        // raw Spring Data repo, for the lazy-load demo

    private static Order sampleOrder() {
        return Order.place("cust-1", List.of(
                new OrderLine("SKU-1", 2, new Money(new BigDecimal("9.99"), EUR)),
                new OrderLine("SKU-2", 1, new Money(new BigDecimal("4.00"), EUR))));
    }

    @Test
    void orderRoundTripsThroughPostgres() {
        Order placed = sampleOrder();
        adapter.save(placed);

        // loaded back from Postgres in a separate call (the "survives" path)
        Order loaded = adapter.findById(placed.id()).orElseThrow();

        assertThat(loaded.customerId()).isEqualTo("cust-1");
        assertThat(loaded.status()).isEqualTo(OrderStatus.PLACED);
        assertThat(loaded.currency()).isEqualTo(EUR);
        assertThat(loaded.lines()).hasSize(2);
        assertThat(loaded.total().amount()).isEqualByComparingTo("23.98");
    }

    @Test
    void adapterEagerlyFetchesLines_safeOutsideSession() {
        Order placed = sampleOrder();
        adapter.save(placed);

        // adapter uses @EntityGraph and maps to a record inside the tx → fully materialised
        Order loaded = adapter.findById(placed.id()).orElseThrow();

        assertThat(loaded.lines()).hasSize(2); // no LazyInitializationException
    }

    @Test
    void lazyLinesThrowOnceSessionClosed_provingOsivOff() {
        Order placed = sampleOrder();
        adapter.save(placed);

        // raw entity loaded WITHOUT the graph; with OSIV off there's no request-scoped
        // session, so findById's own session has already closed and the entity is detached
        OrderEntity detached = jpa.findById(placed.id()).orElseThrow();

        assertThatThrownBy(() -> detached.getLines().size())
                .isInstanceOf(LazyInitializationException.class);
    }
}
