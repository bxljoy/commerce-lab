package com.commercelab.order.service;

import com.commercelab.order.domain.Order;
import com.commercelab.order.domain.OrderNotFoundException;
import com.commercelab.order.domain.OrderStatus;
import com.commercelab.order.inventory.InventoryOutcome;
import com.commercelab.order.persistence.OrderEntity;
import com.commercelab.order.persistence.OrderJpaRepository;
import com.commercelab.order.persistence.OrderRequestStore;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class OrderProgressService {
    private final OrderJpaRepository orders;
    private final OrderRequestStore requests;
    private final Clock clock;
    private final TransactionTemplate transaction;

    public OrderProgressService(OrderJpaRepository orders, OrderRequestStore requests,
            PlatformTransactionManager transactionManager, Clock clock) {
        this.orders = orders;
        this.requests = requests;
        this.clock = clock;
        transaction = new TransactionTemplate(transactionManager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public PendingOrderSnapshot load(UUID id) {
        return transaction.execute(status -> {
            OrderEntity order = find(id);
            return new PendingOrderSnapshot(order.toDomain(), order.getAttemptCount(), requests.findCorrelationId(id));
        });
    }

    /** The coordinator validates response identity/lines before recording a definitive outcome. */
    public Order apply(UUID id, InventoryOutcome outcome) {
        Objects.requireNonNull(outcome, "outcome");
        return update(id, order -> {
            if (outcome instanceof InventoryOutcome.Reserved) order.finalizeInventory(true);
            else if (outcome instanceof InventoryOutcome.Rejected) order.finalizeInventory(false);
        });
    }

    public List<UUID> findDueIds(Instant now, int limit) {
        Objects.requireNonNull(now, "now");
        if (limit < 1) throw new IllegalArgumentException("limit must be positive");
        return transaction.execute(status -> List.copyOf(orders.findDueIds(now, PageRequest.of(0, limit))));
    }

    public void defer(UUID id, String failureCode, Instant nextAttempt) {
        requireStableCode(failureCode);
        Objects.requireNonNull(nextAttempt, "nextAttempt");
        update(id, order -> order.defer(failureCode, nextAttempt, clock.instant()));
    }

    public void deferRecovery(UUID id, String failureCode) {
        requireStableCode(failureCode);
        update(id, order -> {
            // Recompute from the managed row on every optimistic retry, not the pre-HTTP snapshot.
            long seconds = Math.min(60, 5L << Math.min(order.getAttemptCount(), 4));
            Instant now = clock.instant();
            order.defer(failureCode, now.plusSeconds(seconds)
                    .plusMillis(ThreadLocalRandom.current().nextLong(251)), now);
        });
    }

    public void block(UUID id, String issueCode) {
        requireStableCode(issueCode);
        update(id, order -> order.block(issueCode, clock.instant()));
    }

    private Order update(UUID id, Consumer<OrderEntity> change) {
        for (int attempt = 0; ; attempt++) {
            try {
                return transaction.execute(status -> {
                    OrderEntity order = find(id);
                    change.accept(order);
                    return order.toDomain();
                });
            } catch (OptimisticLockingFailureException conflict) {
                // execute has rolled back and closed the failed persistence context.
                // Reapply the guard to fresh state; sustained contention stays retryable.
                if (attempt == 2) {
                    Order current = load(id).order();
                    if (current.status() != OrderStatus.PENDING_INVENTORY) return current;
                    throw conflict;
                }
            }
        }
    }

    private OrderEntity find(UUID id) {
        return orders.findByIdWithLines(id).orElseThrow(() -> new OrderNotFoundException(id));
    }

    private static void requireStableCode(String code) {
        if (code == null || !code.matches("[A-Z][A-Z0-9_]{0,63}")) {
            throw new IllegalArgumentException("recovery code must be a stable uppercase identifier");
        }
    }
}
