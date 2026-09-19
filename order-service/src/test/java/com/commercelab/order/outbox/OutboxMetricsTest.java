package com.commercelab.order.outbox;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class OutboxMetricsTest {
    final OutboxDeliveryStore store = mock(OutboxDeliveryStore.class);
    final MockClock clock = new MockClock();
    final SimpleMeterRegistry meters = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);
    final OutboxMetrics metrics = new OutboxMetrics(store, meters);

    @AfterEach void close() { meters.close(); }

    @Test void unknownUntilRefreshedThenEveryScrapeUsesTheSameCachedValues() {
        assertThat(gauge("outbox.pending")).isNaN();
        assertThat(gauge("outbox.snapshot.age")).isNaN();
        assertThat(gauge("outbox.snapshot.stale")).isEqualTo(1);
        verifyNoInteractions(store);
        when(store.stats()).thenReturn(new OutboxStats(2, 17.5, 1));
        metrics.refresh();
        for (int i = 0; i < 10; i++) {
            assertThat(gauge("outbox.pending")).isEqualTo(2);
            assertThat(gauge("outbox.failed.pending")).isEqualTo(1);
            assertThat(gauge("outbox.oldest.pending.age")).isEqualTo(17.5);
        }
        verify(store, times(1)).stats();
        assertThat(gauge("outbox.snapshot.stale")).isZero();
        clock.addSeconds(3);
        assertThat(gauge("outbox.snapshot.age")).isEqualTo(3);
        when(store.stats()).thenReturn(new OutboxStats(0, 0, 0));
        metrics.refresh();
        assertThat(gauge("outbox.pending")).isZero();
        assertThat(gauge("outbox.snapshot.age")).isZero();
        verify(store, times(2)).stats();
    }

    @Test void failureRetainsLastSnapshotAndTimestampUntilSuccessfulRecovery() {
        when(store.stats()).thenReturn(new OutboxStats(2, 17.5, 1))
                .thenThrow(new IllegalStateException("injected failure"))
                .thenReturn(new OutboxStats(1, 22, 0));
        metrics.refresh();
        clock.addSeconds(5);
        assertThatCode(metrics::refresh).doesNotThrowAnyException();
        for (int i = 0; i < 10; i++) {
            assertThat(gauge("outbox.pending")).isEqualTo(2);
            assertThat(gauge("outbox.failed.pending")).isEqualTo(1);
            assertThat(gauge("outbox.oldest.pending.age")).isEqualTo(17.5);
            assertThat(gauge("outbox.snapshot.stale")).isEqualTo(1);
            assertThat(gauge("outbox.snapshot.age")).isEqualTo(5);
        }
        verify(store, times(2)).stats();
        assertThat(meters.get("outbox.snapshot.refresh.failures").counter().count()).isEqualTo(1);
        metrics.refresh();
        assertThat(gauge("outbox.pending")).isEqualTo(1);
        assertThat(gauge("outbox.failed.pending")).isZero();
        assertThat(gauge("outbox.oldest.pending.age")).isEqualTo(22);
        assertThat(gauge("outbox.snapshot.stale")).isZero();
        assertThat(gauge("outbox.snapshot.age")).isZero();
    }

    @Test void firstRefreshFailureDoesNotReportAnEmptyHealthyQueue() {
        when(store.stats()).thenThrow(new IllegalStateException("db down"));
        metrics.refresh();
        assertThat(gauge("outbox.pending")).isNaN();
        assertThat(gauge("outbox.failed.pending")).isNaN();
        assertThat(gauge("outbox.oldest.pending.age")).isNaN();
        assertThat(gauge("outbox.snapshot.stale")).isEqualTo(1);
        assertThat(gauge("outbox.snapshot.age")).isNaN();
        verify(store).stats();
    }

    @Test void scrapeDoesNotBlockOnRefreshAndSeesOnlyCommittedSnapshots() throws Exception {
        when(store.stats()).thenReturn(new OutboxStats(2, 17.5, 1));
        metrics.refresh();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        when(store.stats()).thenAnswer(invocation -> {
            entered.countDown();
            assertThat(release.await(3, TimeUnit.SECONDS)).isTrue();
            return new OutboxStats(0, 0, 0);
        });
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            var refresh = executor.submit(metrics::refresh);
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(gauge("outbox.pending")).isEqualTo(2);
            assertThat(gauge("outbox.failed.pending")).isEqualTo(1);
            assertThat(gauge("outbox.oldest.pending.age")).isEqualTo(17.5);
            release.countDown();
            refresh.get(3, TimeUnit.SECONDS);
            assertThat(gauge("outbox.pending")).isZero();
            assertThat(gauge("outbox.failed.pending")).isZero();
            assertThat(gauge("outbox.oldest.pending.age")).isZero();
        } finally { release.countDown(); executor.shutdownNow(); }
    }

    @Test void recordsRelayCountersAndTimersWithoutQueryingStore() {
        metrics.recordAttempt("delivered", Duration.ofMillis(25).toNanos());
        metrics.recordAttempt("stale", Duration.ofMillis(10).toNanos());
        metrics.claimFailed();
        assertThat(meters.get("outbox.attempts").tag("outcome", "delivered").counter().count()).isEqualTo(1);
        assertThat(meters.get("outbox.attempts").tag("outcome", "stale").counter().count()).isEqualTo(1);
        assertThat(meters.get("outbox.publication").tag("outcome", "delivered").timer().totalTime(TimeUnit.MILLISECONDS)).isEqualTo(25);
        assertThat(meters.get("outbox.bookkeeping.failures").tag("operation", "claim").counter().count()).isEqualTo(1);
        verifyNoInteractions(store);
    }

    double gauge(String name) { return meters.get(name).gauge().value(); }
}
