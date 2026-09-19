package com.commercelab.order.service;

import java.time.Clock;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;

public class OrderRecoveryWorker {
    private static final Logger log = LoggerFactory.getLogger(OrderRecoveryWorker.class);
    private final OrderProgressService progress;
    private final OrderReservationCoordinator coordinator;
    private final Clock clock;
    private final int batchSize;

    public OrderRecoveryWorker(OrderProgressService progress, OrderReservationCoordinator coordinator,
            Clock clock, int batchSize) {
        if (batchSize < 1) throw new IllegalArgumentException("recovery batch size must be positive");
        this.progress = progress;
        this.coordinator = coordinator;
        this.clock = clock;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${order.recovery.fixed-delay-ms:5000}",
            initialDelayString = "${order.recovery.fixed-delay-ms:5000}")
    public void runOnce() {
        // The ID-only scan transaction is closed before loading snapshots or calling inventory.
        for (UUID id : progress.findDueIds(clock.instant(), batchSize)) {
            try {
                coordinator.reconcile(id);
            } catch (RuntimeException failure) {
                log.error("orderId={} operation=reconcile transition=DEFERRED failureCode=RECOVERY_PASS_FAILED exceptionType={}",
                        id, failure.getClass().getSimpleName());
            } finally {
                MDC.clear();
            }
        }
    }
}
