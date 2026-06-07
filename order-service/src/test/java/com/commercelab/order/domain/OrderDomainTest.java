package com.commercelab.order.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Currency;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pure domain unit tests — no Spring, no infrastructure. Exercises the records-as-value-
 * objects half of the records note: invariants, value equality, and defensive copying.
 */
class OrderDomainTest {

    private static final Currency EUR = Currency.getInstance("EUR");

    @Test
    void moneyTimesAndPlusCompute() {
        Money unit = new Money(new BigDecimal("9.99"), EUR);
        assertThat(unit.times(3).amount()).isEqualByComparingTo("29.97");
        assertThat(unit.plus(new Money(new BigDecimal("0.01"), EUR)).amount()).isEqualByComparingTo("10.00");
    }

    @Test
    void moneyRejectsNegativeAmount() {
        assertThatThrownBy(() -> new Money(new BigDecimal("-1"), EUR))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void moneyPlusRejectsCurrencyMismatch() {
        Money eur = new Money(BigDecimal.ONE, EUR);
        Money usd = new Money(BigDecimal.ONE, Currency.getInstance("USD"));
        assertThatThrownBy(() -> eur.plus(usd)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void orderLineRejectsNonPositiveQuantity() {
        assertThatThrownBy(() -> new OrderLine("SKU-1", 0, new Money(BigDecimal.ONE, EUR)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void orderTotalSumsLineTotals() {
        Order order = Order.place("cust-1", List.of(
                new OrderLine("SKU-1", 2, new Money(new BigDecimal("9.99"), EUR)),    // 19.98
                new OrderLine("SKU-2", 1, new Money(new BigDecimal("4.00"), EUR))));  // 4.00

        assertThat(order.total().amount()).isEqualByComparingTo("23.98");
        assertThat(order.status()).isEqualTo(OrderStatus.PLACED);
        assertThat(order.id()).isNotNull();
    }

    @Test
    void orderRejectsEmptyLines() {
        assertThatThrownBy(() -> Order.place("cust-1", List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void orderDefensivelyCopiesLines() {
        List<OrderLine> lines = new ArrayList<>();
        lines.add(new OrderLine("SKU-1", 1, new Money(BigDecimal.ONE, EUR)));
        Order order = Order.place("cust-1", lines);

        lines.add(new OrderLine("SKU-2", 9, new Money(BigDecimal.TEN, EUR))); // mutate source after construction

        assertThat(order.lines()).hasSize(1); // record kept its own copy — unaffected
    }
}
