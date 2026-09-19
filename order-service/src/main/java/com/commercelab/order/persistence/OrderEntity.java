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
import jakarta.persistence.OrderBy;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
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
    @Column(nullable = false, length = 32)
    private OrderStatus status;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "placed_at", nullable = false)
    private Instant placedAt;

    @Version
    @Column(nullable = false)
    private Long version;

    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    @Column(name = "last_attempt_at")
    private Instant lastAttemptAt;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "recovery_blocked", nullable = false)
    private boolean recoveryBlocked;

    @Column(name = "last_failure_code", length = 64)
    private String lastFailureCode;

    @Column(name = "rejection_reason", length = 64)
    private String rejectionReason;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    @OrderBy("position ASC")
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
        entity.rejectionReason = order.rejectionReason();
        if (entity.status == OrderStatus.PENDING_INVENTORY) {
            entity.nextAttemptAt = order.placedAt().plusSeconds(5);
        }
        for (int position = 0; position < order.lines().size(); position++) {
            entity.lines.add(OrderLineEntity.fromDomain(order.lines().get(position), entity, position));
        }
        return entity;
    }

    public Order toDomain() {
        Currency cur = Currency.getInstance(currency);
        List<OrderLine> domainLines = lines.stream()
                .map(line -> line.toDomain(cur))
                .toList();
        return new Order(id, customerId, status, domainLines, placedAt,
                rejectionReason, recoveryBlocked ? lastFailureCode : null);
    }

    public void finalizeInventory(boolean reserved) {
        if (status != OrderStatus.PENDING_INVENTORY) return;
        status = reserved ? OrderStatus.CONFIRMED : OrderStatus.REJECTED;
        rejectionReason = reserved ? null : "STOCK_UNAVAILABLE";
        nextAttemptAt = null;
        recoveryBlocked = false;
        lastFailureCode = null;
    }

    public void defer(String failureCode, Instant nextAttempt, Instant now) {
        if (status != OrderStatus.PENDING_INVENTORY || recoveryBlocked) return;
        attemptCount++;
        lastAttemptAt = now;
        lastFailureCode = failureCode;
        nextAttemptAt = nextAttempt;
    }

    public void block(String issueCode, Instant now) {
        if (status != OrderStatus.PENDING_INVENTORY || recoveryBlocked) return;
        attemptCount++;
        lastAttemptAt = now;
        recoveryBlocked = true;
        lastFailureCode = issueCode;
        nextAttemptAt = null;
    }

    public int getAttemptCount() { return attemptCount; }

    public UUID getId() {
        return id;
    }

    public List<OrderLineEntity> getLines() {
        return lines;
    }
}
