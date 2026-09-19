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
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * Testcontainers-backed persistence integration test (real Postgres, Flyway-migrated,
 * Hibernate validating). Proves the order round-trips through the database, that the
 * adapter's explicit fetch makes the result safe to use after the session closes, and
 * that a naive lazy load fails once its short-lived repository session ends. OSIV is
 * guarded separately at the application-context level because it is HTTP-scoped.
 */
@SpringBootTest
@TestPropertySource(properties = {
        "spring.jpa.properties.hibernate.generate_statistics=true",
        "spring.jpa.properties.hibernate.jdbc.batch_size=0"
})
class OrderPersistenceIT extends AbstractPostgresIntegrationTest {

    private static final Currency EUR = Currency.getInstance("EUR");

    @Autowired
    private JpaOrderRepository adapter;   // domain port (maps entity <-> domain record)

    @Autowired
    private OrderJpaRepository jpa;        // raw Spring Data repo, for the lazy-load demo

    @Autowired
    private jakarta.persistence.EntityManagerFactory entityManagerFactory;

    private static Order sampleOrder() {
        return Order.place("cust-1", List.of(
                new OrderLine("SKU-1", 2, new Money(new BigDecimal("9.99"), EUR)),
                new OrderLine("SKU-2", 1, new Money(new BigDecimal("4.00"), EUR))));
    }

    @Test
    void orderRoundTripsThroughPostgres() {
        Order placed = sampleOrder();
        adapter.add(placed);

        // loaded back from Postgres in a separate call (the "survives" path)
        Order loaded = adapter.findById(placed.id()).orElseThrow();

        assertThat(loaded.customerId()).isEqualTo("cust-1");
        assertThat(loaded.status()).isEqualTo(OrderStatus.PENDING_INVENTORY);
        assertThat(loaded.currency()).isEqualTo(EUR);
        assertThat(loaded.lines()).extracting(OrderLine::sku)
                .containsExactly("SKU-1", "SKU-2");
        assertThat(loaded.total().amount()).isEqualByComparingTo("23.98");
    }

    @Test
    void assignedIdInsertUsesPersistWithoutLookupSelect() {
        Statistics statistics = entityManagerFactory.unwrap(SessionFactory.class).getStatistics();
        statistics.clear();

        adapter.add(sampleOrder());

        assertThat(statistics.getEntityInsertCount()).isEqualTo(3);
        assertThat(statistics.getEntityLoadCount()).isZero();
        assertThat(statistics.getQueryExecutionCount()).isZero();
        assertThat(statistics.getPrepareStatementCount()).isEqualTo(3);
    }

    @Test
    void adapterEagerlyFetchesLines_safeOutsideSession() {
        Order placed = sampleOrder();
        adapter.add(placed);

        // adapter uses @EntityGraph and maps to a record inside the tx → fully materialised
        Order loaded = adapter.findById(placed.id()).orElseThrow();

        assertThat(loaded.lines()).hasSize(2); // no LazyInitializationException
    }

    @Test
    void detachedLazyCollectionThrowsOutsideTransaction() {
        Order placed = sampleOrder();
        adapter.add(placed);

        // Raw entity loaded without the graph. The repository transaction has ended,
        // so the entity is detached and its lazy collection cannot initialize.
        OrderEntity detached = jpa.findById(placed.id()).orElseThrow();

        assertThatThrownBy(() -> detached.getLines().size())
                .isInstanceOf(LazyInitializationException.class);
    }
}
