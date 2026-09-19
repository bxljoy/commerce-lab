package com.commercelab.order.service;

import com.commercelab.order.domain.Order;
import com.commercelab.order.domain.OrderStatus;
import com.commercelab.order.inventory.*;
import com.commercelab.order.web.CorrelationIdFilter;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderReservationCoordinator {
    private static final Logger log = LoggerFactory.getLogger(OrderReservationCoordinator.class);
    private final OrderProgressService progress;
    private final InventoryGateway gateway;
    private final Clock clock;

    public OrderReservationCoordinator(OrderProgressService progress, InventoryGateway gateway, Clock clock) {
        this.progress = progress;
        this.gateway = gateway;
        this.clock = clock;
    }

    @Transactional(propagation = Propagation.NEVER)
    public Order attempt(UUID id) {
        PendingOrderSnapshot snapshot = progress.load(id);
        if (snapshot.order().status() != OrderStatus.PENDING_INVENTORY) return snapshot.order();
        var request = new InventoryRequest(id, snapshot.order().lines().stream()
                .map(line -> new InventoryLine(line.sku(), line.quantity())).toList());
        String correlation = CorrelationIdFilter.validOrNew(snapshot.correlationId());
        String previous = MDC.get("correlationId");
        MDC.put("correlationId", correlation);
        try {
            for (int attempt = 1; attempt <= 2; attempt++) {
                long start = System.nanoTime();
                try {
                    InventoryOutcome outcome = gateway.reserve(request, correlation);
                    if (outcome instanceof InventoryOutcome.Reserved reserved) {
                        InventoryResponseValidation.requireMatch(request, reserved.orderId(), reserved.lines());
                    } else if (outcome instanceof InventoryOutcome.Released) {
                        throw new InventoryProtocolException("INVENTORY_RELEASED");
                    } else if (!(outcome instanceof InventoryOutcome.Rejected)) {
                        throw new InventoryProtocolException("INVENTORY_INVALID_RESPONSE");
                    }
                    Order order = progress.apply(id, outcome);
                    log(id, correlation, attempt, start, order.status().name(), "NONE");
                    return order;
                } catch (InventoryProtocolException ex) {
                    progress.block(id, ex.code());
                    log(id, correlation, attempt, start, "BLOCKED", ex.code());
                    return progress.load(id).order();
                } catch (TransientInventoryException ex) {
                    log(id, correlation, attempt, start, "PENDING_INVENTORY", ex.code());
                    if (attempt == 2 || ex.code().equals("INVENTORY_CIRCUIT_OPEN")) {
                        return defer(id, ex.code());
                    }
                    try { Thread.sleep(100); }
                    catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return defer(id, "INVENTORY_INTERRUPTED");
                    }
                }
            }
            throw new IllegalStateException("unreachable attempt budget");
        } finally {
            if (previous == null) MDC.remove("correlationId");
            else MDC.put("correlationId", previous);
        }
    }

    private Order defer(UUID id, String code) {
        progress.defer(id, code, clock.instant().plusSeconds(5));
        return progress.load(id).order();
    }

    @Transactional(propagation = Propagation.NEVER)
    public void reconcile(UUID id) {
        PendingOrderSnapshot snapshot = progress.load(id);
        if (snapshot.order().status() != OrderStatus.PENDING_INVENTORY
                || snapshot.order().recoveryIssue() != null) return;
        var request = new InventoryRequest(id, snapshot.order().lines().stream()
                .map(line -> new InventoryLine(line.sku(), line.quantity())).toList());
        String correlation = CorrelationIdFilter.validOrNew(snapshot.correlationId());
        String previous = MDC.get("correlationId");
        MDC.put("correlationId", correlation);
        long start = System.nanoTime();
        String operation = "find";
        try {
            InventoryOutcome outcome = gateway.find(id, correlation);
            if (outcome instanceof InventoryOutcome.Missing) {
                recoveryLog(id, correlation, operation, snapshot.attemptCount(), start, "MISSING", "NONE");
                operation = "reserve";
                start = System.nanoTime();
                outcome = gateway.reserve(request, correlation);
            }
            if (outcome instanceof InventoryOutcome.Reserved reserved) {
                InventoryResponseValidation.requireMatch(request, reserved.orderId(), reserved.lines());
            } else if (outcome instanceof InventoryOutcome.Released) {
                throw new InventoryProtocolException("INVENTORY_RELEASED");
            } else if (!(outcome instanceof InventoryOutcome.Rejected)) {
                throw new InventoryProtocolException("INVENTORY_INVALID_RESPONSE");
            }
            Order order = progress.apply(id, outcome);
            recoveryLog(id, correlation, operation, snapshot.attemptCount(), start, order.status().name(), "NONE");
        } catch (InventoryProtocolException ex) {
            progress.block(id, ex.code());
            recoveryLog(id, correlation, operation, snapshot.attemptCount(), start, "BLOCKED", ex.code());
        } catch (TransientInventoryException ex) {
            // attemptCount counts completed failed/deferred passes, not physical HTTP calls.
            long seconds = Math.min(60, 5L << Math.min(snapshot.attemptCount(), 4));
            progress.defer(id, ex.code(), clock.instant().plusSeconds(seconds)
                    .plusMillis(ThreadLocalRandom.current().nextLong(251)));
            recoveryLog(id, correlation, operation, snapshot.attemptCount(), start, "PENDING_INVENTORY", ex.code());
        } finally {
            if (previous == null) MDC.remove("correlationId");
            else MDC.put("correlationId", previous);
        }
    }

    private static void recoveryLog(UUID id, String correlation, String operation, int failedPasses,
            long start, String transition, String code) {
        log.info("orderId={} correlationId={} operation=reconcile.{} attempt={} transition={} latencyMs={} failureCode={}",
                id, correlation, operation, (long) failedPasses + 1, transition,
                (System.nanoTime() - start) / 1_000_000, code);
    }

    private static void log(UUID id, String correlation, int attempt, long start, String transition, String code) {
        log.info("orderId={} correlationId={} operation=reserve attempt={} transition={} latencyMs={} failureCode={}",
                id, correlation, attempt, transition, (System.nanoTime() - start) / 1_000_000, code);
    }
}
