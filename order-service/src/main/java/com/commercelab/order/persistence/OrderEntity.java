package com.commercelab.order.persistence;

import com.commercelab.order.domain.Order;
import com.commercelab.order.domain.OrderLine;
import com.commercelab.order.domain.OrderStatus;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Currency;
import java.util.List;
import java.util.UUID;

/**
 * JPA persistence model for an order. Deliberately separate from the domain
 * {@link Order} record: this is a mutable, no-arg-constructable Hibernate entity, while
 * the domain stays an immutable record. Mapping happens here ({@link #fromDomain} /
 * {@link #toDomain}); entities never leak past the repository adapter.
 *
 * <p>The id is an application-assigned UUID (minted in {@code Order.place}), never a
 * {@code @GeneratedValue} — so it's non-null and stable from construction, which is the
 * safe story for entity identity. We don't override equals/hashCode (the senior default)
 * and use a {@code List} (not a {@code Set}) for lines, sidestepping the HashSet trap.
 */
@Entity
@Table(name = "orders")
public class OrderEntity {

    @Id
    private UUID id;

    @Column(name = "customer_id", nullable = false, length = 64)
    private String customerId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private OrderStatus status;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "placed_at", nullable = false)
    private Instant placedAt;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderLineEntity> lines = new ArrayList<>();

    protected OrderEntity() {
        // for JPA
    }

    public static OrderEntity fromDomain(Order order) {
        OrderEntity entity = new OrderEntity();
        entity.id = order.id();
        entity.customerId = order.customerId();
        entity.status = order.status();
        entity.currency = order.currency().getCurrencyCode();
        entity.placedAt = order.placedAt();
        for (OrderLine line : order.lines()) {
            entity.lines.add(OrderLineEntity.fromDomain(line, entity));
        }
        return entity;
    }

    public Order toDomain() {
        Currency cur = Currency.getInstance(currency);
        List<OrderLine> domainLines = lines.stream()
                .map(line -> line.toDomain(cur))
                .toList();
        return new Order(id, customerId, status, domainLines, placedAt);
    }

    public UUID getId() {
        return id;
    }

    public List<OrderLineEntity> getLines() {
        return lines;
    }
}
