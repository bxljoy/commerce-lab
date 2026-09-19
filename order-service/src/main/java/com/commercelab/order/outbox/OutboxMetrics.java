package com.commercelab.order.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboxMetrics {
    private static final Logger log = LoggerFactory.getLogger(OutboxMetrics.class);
    private record Snapshot(OutboxStats stats, long refreshedAtNanos, boolean stale) {}

    private final OutboxDeliveryStore store;
    private final MeterRegistry meters;
    private volatile Snapshot snapshot = new Snapshot(null, 0, true);

    public OutboxMetrics(OutboxDeliveryStore store, MeterRegistry meters) {
        this.store = store;
        this.meters = meters;
        Gauge.builder("outbox.pending", this, m -> m.value(OutboxStats::pendingCount)).register(meters);
        Gauge.builder("outbox.failed.pending", this, m -> m.value(OutboxStats::failedPendingCount)).register(meters);
        Gauge.builder("outbox.oldest.pending.age", this, m -> m.value(OutboxStats::oldestPendingAgeSeconds))
                .baseUnit("seconds").register(meters);
        Gauge.builder("outbox.snapshot.stale", this, m -> m.snapshot.stale() ? 1 : 0).register(meters);
        Gauge.builder("outbox.snapshot.age", this, OutboxMetrics::snapshotAge).baseUnit("seconds").register(meters);
    }

    /** Only refresh touches the DB; a slow query never blocks gauge observations. */
    public synchronized void refresh() {
        try {
            OutboxStats stats = java.util.Objects.requireNonNull(store.stats());
            snapshot = new Snapshot(stats, meters.config().clock().monotonicTime(), false);
        } catch (RuntimeException ex) {
            Snapshot previous = snapshot;
            snapshot = new Snapshot(previous.stats(), previous.refreshedAtNanos(), true);
            meters.counter("outbox.snapshot.refresh.failures").increment();
            log.warn("Outbox metrics outcome=stale code=METRICS_REFRESH_FAILED");
        }
    }

    public void recordAttempt(String outcome, long elapsedNanos) {
        meters.counter("outbox.attempts", "outcome", outcome).increment();
        meters.timer("outbox.publication", "outcome", outcome).record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    public void claimFailed() {
        meters.counter("outbox.bookkeeping.failures", "operation", "claim").increment();
    }

    private double value(ToDoubleFunction<OutboxStats> measurement) {
        OutboxStats stats = snapshot.stats();
        return stats == null ? Double.NaN : measurement.applyAsDouble(stats);
    }

    private double snapshotAge() {
        Snapshot current = snapshot;
        return current.stats() == null ? Double.NaN
                : (meters.config().clock().monotonicTime() - current.refreshedAtNanos()) / 1_000_000_000.0;
    }
}
