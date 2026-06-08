package com.commercelab.order.persistence;

import com.commercelab.order.domain.Money;
import com.commercelab.order.domain.OrderLine;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.util.Currency;
import java.util.UUID;

/**
 * Persistence model for a single order line. The {@code @ManyToOne} back-reference to the
 * order is {@code LAZY} (the spec default for {@code @ManyToOne} is EAGER — the opposite of
 * what you want); reads opt into eager loading per-query via {@code @EntityGraph}.
 *
 * <p>The line's currency is not stored here — it's carried by the parent order (all lines
 * share one currency), so {@link #toDomain} reconstructs the {@link Money} from the order's
 * currency.
 */
@Entity
@Table(name = "order_lines")
public class OrderLineEntity {

    @Id
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "order_id", nullable = false)
    private OrderEntity order;

    @Column(nullable = false, length = 64)
    private String sku;

    @Column(nullable = false)
    private int quantity;

    @Column(name = "unit_price", nullable = false, precision = 19, scale = 4)
    private BigDecimal unitPrice;

    protected OrderLineEntity() {
        // for JPA
    }

    static OrderLineEntity fromDomain(OrderLine line, OrderEntity parent) {
        OrderLineEntity entity = new OrderLineEntity();
        entity.id = UUID.randomUUID();
        entity.order = parent;
        entity.sku = line.sku();
        entity.quantity = line.quantity();
        entity.unitPrice = line.unitPrice().amount();
        return entity;
    }

    OrderLine toDomain(Currency currency) {
        return new OrderLine(sku, quantity, new Money(unitPrice, currency));
    }
}
