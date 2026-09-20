package com.commercelab.inventory.outbox;

import static org.assertj.core.api.Assertions.*;

import com.commercelab.inventory.AbstractPostgresIntegrationTest;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(properties = "inventory.outbox.enabled=false")
@org.junit.jupiter.api.Timeout(60)
class OutboxDeliveryStoreIT extends AbstractPostgresIntegrationTest {
    private static final Duration LEASE = Duration.ofSeconds(60);
    @Autowired JdbcTemplate jdbc;
    @Autowired com.commercelab.inventory.messaging.InventoryEventHandler handler;
    @Autowired OutboxDeliveryStore store;
    @Autowired PlatformTransactionManager transactionManager;

    @BeforeEach void clear() {
        jdbc.execute("TRUNCATE inventory_event_inbox, inventory_reservation_attempts, inventory_reservations, stock CASCADE");
    }

    @Test void onlyDueUndeliveredUnleasedRowsAreClaimed() {
        OutboxMessage later = seed();
        jdbc.update("UPDATE inventory_result_outbox SET next_attempt_at=clock_timestamp()+interval '1 hour' WHERE event_id=?",
                later.eventId());
        OutboxMessage delivered = seed();
        jdbc.update("UPDATE inventory_result_outbox SET delivered_at=clock_timestamp() WHERE event_id=?", delivered.eventId());
        OutboxMessage due = seed();
        OutboxClaim claim = store.claimNext(LEASE).orElseThrow();
        assertThat(claim.message()).isEqualTo(due);
        assertThat(claim.attemptCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT lease_until > clock_timestamp() FROM inventory_result_outbox WHERE event_id=?",
                Boolean.class, due.eventId())).isTrue();
        assertThat(store.claimNext(LEASE)).isEmpty();
        assertThat(store.markDelivered(due.eventId(), claim.token())).isTrue();
        assertThat(store.markDelivered(due.eventId(), claim.token())).isFalse();
        assertThat(store.reschedule(due.eventId(), claim.token(), Duration.ofSeconds(1), "SEND_FAILED")).isFalse();
        assertThat(store.claimNext(LEASE)).isEmpty();
        assertMessage(due);
    }

    @Test void simultaneousClaimsHaveOnlyOneWinnerForOneRow() throws Exception {
        seed();
        var barrier = new CyclicBarrier(3);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> claimAt(barrier));
            var second = workers.submit(() -> claimAt(barrier));
            barrier.await(10, TimeUnit.SECONDS);
            var results = java.util.stream.Stream.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS))
                    .flatMap(Optional::stream).toList();
            assertThat(results).hasSize(1);
            assertThat(results.getFirst().attemptCount()).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT attempt_count FROM inventory_result_outbox", Long.class)).isEqualTo(1);
        }
    }

    @Test void simultaneousClaimsOfDifferentRowsHaveDifferentLiveTokens() throws Exception {
        seed();
        seed();
        var barrier = new CyclicBarrier(3);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> claimAt(barrier));
            var second = workers.submit(() -> claimAt(barrier));
            barrier.await(10, TimeUnit.SECONDS);
            OutboxClaim a = first.get(10, TimeUnit.SECONDS).orElseThrow();
            OutboxClaim b = second.get(10, TimeUnit.SECONDS).orElseThrow();
            assertThat(a.message().eventId()).isNotEqualTo(b.message().eventId());
            assertThat(a.token()).isNotEqualTo(b.token());
            assertThat(a.attemptCount()).isEqualTo(1);
            assertThat(b.attemptCount()).isEqualTo(1);
            assertThat(store.claimNext(LEASE)).isEmpty();
        }
    }

    @Test void skipsLockedCandidateWithoutWaitingForItsTransaction() throws Exception {
        OutboxMessage locked = seed();
        OutboxMessage available = seed();
        jdbc.update("UPDATE inventory_result_outbox SET next_attempt_at=clock_timestamp()-interval '1 hour' WHERE event_id=?",
                locked.eventId());
        var barrier = new CyclicBarrier(2);
        try (var worker = Executors.newSingleThreadExecutor()) {
            new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
                jdbc.queryForObject("SELECT event_id FROM inventory_result_outbox WHERE event_id=? FOR UPDATE",
                        UUID.class, locked.eventId());
                var result = worker.submit(() -> claimAt(barrier));
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    assertThat(result.get(10, TimeUnit.SECONDS).orElseThrow().message()).isEqualTo(available);
                } catch (Exception ex) {
                    throw new AssertionError(ex);
                }
            });
        }
    }

    @Test void expiredLeaseFixtureReclaimsAndRejectsBothStaleCompletionPaths() {
        OutboxMessage message = seed();
        OutboxClaim first = store.claimNext(LEASE).orElseThrow();
        expireLease(message.eventId());
        OutboxClaim second = store.claimNext(LEASE).orElseThrow();
        assertThat(second.token()).isNotEqualTo(first.token());
        assertThat(second.attemptCount()).isEqualTo(2);
        var before = jdbc.queryForMap("SELECT * FROM inventory_result_outbox WHERE event_id=?", message.eventId());
        assertThat(store.markDelivered(message.eventId(), first.token())).isFalse();
        assertThat(store.reschedule(message.eventId(), first.token(), Duration.ofSeconds(1), "SEND_FAILED")).isFalse();
        assertThat(jdbc.queryForMap("SELECT * FROM inventory_result_outbox WHERE event_id=?", message.eventId())).isEqualTo(before);
        assertThat(store.markDelivered(message.eventId(), second.token())).isTrue();
        assertClearedLease(message.eventId());
        assertMessage(message);
    }

    @Test void repeatedFailureIsDeferredSoAnotherDueRowCanProgress() {
        OutboxMessage failing = seed();
        for (int attempt = 1; attempt <= 3; attempt++) {
            OutboxClaim claim = store.claimNext(LEASE).orElseThrow();
            assertThat(claim.attemptCount()).isEqualTo(attempt);
            assertThat(store.reschedule(failing.eventId(), claim.token(),
                    new OutboxRetryPolicy(() -> 250).delay(attempt), "SEND_FAILED")).isTrue();
            assertThat(jdbc.queryForObject("SELECT next_attempt_at > clock_timestamp() FROM inventory_result_outbox WHERE event_id=?",
                    Boolean.class, failing.eventId())).isTrue();
            assertClearedLease(failing.eventId());
            assertThat(store.claimNext(LEASE)).isEmpty();
            OutboxMessage other = seed();
            OutboxClaim otherClaim = store.claimNext(LEASE).orElseThrow();
            assertThat(otherClaim.message()).isEqualTo(other);
            assertThat(store.markDelivered(other.eventId(), otherClaim.token())).isTrue();
            // SQL due-time fixture avoids sleeping through the retry delay.
            jdbc.update("UPDATE inventory_result_outbox SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE event_id=?",
                    failing.eventId());
        }
        assertThat(store.stats().failedPendingCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT last_error_code FROM inventory_result_outbox WHERE event_id=?",
                String.class, failing.eventId())).isEqualTo("SEND_FAILED");
        OutboxClaim finalClaim = store.claimNext(LEASE).orElseThrow();
        assertThat(store.markDelivered(failing.eventId(), finalClaim.token())).isTrue();
        assertThat(jdbc.queryForObject("SELECT last_error_code FROM inventory_result_outbox WHERE event_id=?",
                String.class, failing.eventId())).isNull();
        assertThat(store.stats().failedPendingCount()).isZero();
        assertMessage(failing);
    }

    @Test void attemptCountSaturatesInsteadOfOverflowing() {
        OutboxMessage message = seed();
        jdbc.update("UPDATE inventory_result_outbox SET attempt_count=? WHERE event_id=?", Long.MAX_VALUE - 1, message.eventId());
        assertThat(store.claimNext(LEASE).orElseThrow().attemptCount()).isEqualTo(Long.MAX_VALUE);
        expireLease(message.eventId());
        assertThat(store.claimNext(LEASE).orElseThrow().attemptCount()).isEqualTo(Long.MAX_VALUE);
    }

    @Test void publisherFailureKeepsStoredIdentityAndPayloadForRetry() {
        OutboxMessage message = seed();
        var before = jdbc.queryForMap("SELECT event_id, order_id, causation_id, topic, message_key, payload, created_at "
                + "FROM inventory_result_outbox WHERE event_id=?", message.eventId());
        var sent = new java.util.ArrayList<OutboxMessage>();
        var meters = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        try {
            var relay = new OutboxRelay(store, new OutboxRetryPolicy(() -> 0), m -> {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                sent.add(m);
                throw new OutboxPublishException("SEND_FAILED", null);
            }, m -> {}, OutboxProperties.defaults(), new OutboxMetrics(store, meters),
                    new com.fasterxml.jackson.databind.ObjectMapper());
            assertThat(relay.runOnce()).isEqualTo(1);
            jdbc.update("UPDATE inventory_result_outbox SET next_attempt_at=clock_timestamp()-interval '1 second'");
            assertThat(relay.runOnce()).isEqualTo(1);
        } finally { meters.close(); }
        assertThat(sent).containsExactly(message, message);
        assertThat(jdbc.queryForMap("SELECT event_id, order_id, causation_id, topic, message_key, payload, created_at "
                + "FROM inventory_result_outbox WHERE event_id=?", message.eventId())).isEqualTo(before);
        assertThat(jdbc.queryForMap("SELECT delivered_at, attempt_count, last_error_code FROM inventory_result_outbox"))
                .containsEntry("delivered_at", null).containsEntry("attempt_count", 2L)
                .containsEntry("last_error_code", "SEND_FAILED");
    }

    @Test void statsIncludeLiveExpiredAndDeferredClaimsButExcludeDelivered() {
        assertThat(store.stats()).isEqualTo(new OutboxStats(0, 0, 0));
        OutboxMessage expired = seed();
        store.claimNext(LEASE).orElseThrow();
        OutboxMessage live = seed();
        store.claimNext(LEASE).orElseThrow();
        expireLease(expired.eventId());
        OutboxMessage deferred = seed();
        jdbc.update("UPDATE inventory_result_outbox SET next_attempt_at=clock_timestamp()+interval '1 hour', "
                + "last_error_code='SEND_FAILED' WHERE event_id=?", deferred.eventId());
        OutboxMessage delivered = seed();
        jdbc.update("UPDATE inventory_result_outbox SET delivered_at=clock_timestamp(), last_error_code='SEND_FAILED', "
                + "created_at=clock_timestamp()-interval '1 day' WHERE event_id=?", delivered.eventId());
        jdbc.update("UPDATE inventory_result_outbox SET created_at=clock_timestamp()-interval '2 minutes' WHERE event_id=?",
                live.eventId());
        OutboxStats stats = store.stats();
        assertThat(stats.pendingCount()).isEqualTo(3);
        assertThat(stats.failedPendingCount()).isEqualTo(1);
        assertThat(stats.oldestPendingAgeSeconds()).isBetween(120.0, 130.0);
    }

    @Test void operationsCommitIndependentlyOfCallerRollbackAndLeaveNoTransactionOpen() {
        OutboxMessage message = seed();
        var outer = new TransactionTemplate(transactionManager);
        OutboxClaim claim = outer.execute(tx -> {
            OutboxClaim result = store.claimNext(LEASE).orElseThrow();
            tx.setRollbackOnly();
            return result;
        });
        assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
        assertThat(store.claimNext(LEASE)).isEmpty();
        outer.executeWithoutResult(tx -> {
            assertThat(store.reschedule(message.eventId(), claim.token(), Duration.ofHours(1), "SEND_FAILED")).isTrue();
            tx.setRollbackOnly();
        });
        assertClearedLease(message.eventId());
        assertThat(store.claimNext(LEASE)).isEmpty();
        // stats must suspend the outer transaction and cannot see its uncommitted write.
        outer.executeWithoutResult(tx -> {
            jdbc.update("UPDATE inventory_result_outbox SET delivered_at=clock_timestamp() WHERE event_id=?", message.eventId());
            assertThat(store.stats().pendingCount()).isEqualTo(1);
            tx.setRollbackOnly();
        });
        jdbc.update("UPDATE inventory_result_outbox SET next_attempt_at=clock_timestamp() WHERE event_id=?", message.eventId());
        OutboxClaim next = store.claimNext(LEASE).orElseThrow();
        outer.executeWithoutResult(tx -> {
            assertThat(store.markDelivered(message.eventId(), next.token())).isTrue();
            tx.setRollbackOnly();
        });
        assertThat(store.stats().pendingCount()).isZero();
        assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
    }

    private Optional<OutboxClaim> claimAt(CyclicBarrier barrier) throws Exception {
        barrier.await(10, TimeUnit.SECONDS);
        var result = store.claimNext(LEASE);
        assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
        return result;
    }

    private void expireLease(UUID id) {
        // Forced SQL lease-expiration fixture, not a process-crash experiment.
        jdbc.update("UPDATE inventory_result_outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=?", id);
    }

    private void assertClearedLease(UUID id) {
        var row = jdbc.queryForMap("SELECT lease_token, lease_until FROM inventory_result_outbox WHERE event_id=?", id);
        assertThat(row.values()).containsOnlyNulls();
    }

    private void assertMessage(OutboxMessage message) {
        var row = jdbc.queryForMap("SELECT * FROM inventory_result_outbox WHERE event_id=?", message.eventId());
        assertThat(row.get("order_id")).isEqualTo(message.orderId());
        assertThat(row.get("topic")).isEqualTo(message.topic());
        assertThat(row.get("message_key")).isEqualTo(message.messageKey());
        assertThat(row.get("payload")).isEqualTo(message.payload());
    }

    private OutboxMessage seed() {
        return InventoryOutboxTestSupport.seed(handler, jdbc, false, false);
    }
}
