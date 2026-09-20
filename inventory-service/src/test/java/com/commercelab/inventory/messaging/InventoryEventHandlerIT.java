package com.commercelab.inventory.messaging;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doAnswer;

import com.commercelab.inventory.AbstractPostgresIntegrationTest;
import com.commercelab.inventory.domain.ReservationStatus;
import com.commercelab.inventory.domain.ReservationPayloadConflictException;
import com.commercelab.inventory.events.EventJson;
import com.commercelab.inventory.events.EventProtocolException;
import com.commercelab.inventory.events.InventoryResult;
import com.commercelab.inventory.outbox.InventoryResultStore;
import com.commercelab.inventory.service.InventoryService;
import com.commercelab.inventory.service.ReserveInventoryCommand;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.AopTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(properties = {"inventory.events.enabled=false", "spring.kafka.listener.auto-startup=false"})
class InventoryEventHandlerIT extends AbstractPostgresIntegrationTest {
    @Autowired JdbcTemplate jdbc;
    @Autowired InventoryEventHandler handler;
    @Autowired InventoryService inventory;
    @Autowired EventJson events;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired InboxStore inbox;
    @SpyBean InventoryResultStore results;
    private ExecutorService workers;

    @BeforeEach
    void reset() {
        jdbc.execute("TRUNCATE inventory_event_inbox, inventory_reservation_attempts, "
                + "inventory_reservations, stock CASCADE");
        jdbc.update("INSERT INTO stock VALUES ('APPLE', 10), ('BREAD', 10)");
        workers = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void stop() throws Exception {
        workers.shutdownNow();
        assertThat(workers.awaitTermination(5, SECONDS)).isTrue();
    }

    @Test
    void duplicateHasOneBusinessEffectAndImmutableResult() throws Exception {
        UUID order = UUID.randomUUID();
        String body = placed(order, UUID.randomUUID(), 2);
        assertThat(handler.handle(order.toString(), body)).isEqualTo(ProcessingOutcome.APPLIED);
        var row = jdbc.queryForMap("SELECT * FROM inventory_result_outbox");
        assertThat(handler.handle(order.toString(), body)).isEqualTo(ProcessingOutcome.DUPLICATE);
        assertThat(handler.handle(order.toString(), " \n" + body)).isEqualTo(ProcessingOutcome.DUPLICATE);
        assertThat(jdbc.queryForMap("SELECT * FROM inventory_result_outbox")).isEqualTo(row);
        assertThat(count("inventory_event_inbox")).isOne();
        assertThat(count("inventory_result_outbox")).isOne();
        assertThat(inventory.getStock("APPLE").availableQuantity()).isEqualTo(8);
        InventoryResult result = result(order);
        assertThat(result.eventType()).isEqualTo("InventoryReserved");
        assertThat(result.causationId()).isEqualTo(events.readOrderPlaced(order.toString(), body).eventId());
        assertThat(result.correlationId()).isEqualTo(events.readOrderPlaced(order.toString(), body).correlationId());
        assertThat(result.reasonCode()).isNull();
        assertThat(result.shortages()).isNull();
    }

    @Test
    void rejectionIsDurableWithoutReservationOrStockMutation() throws Exception {
        UUID order = UUID.randomUUID();
        handler.handle(order.toString(), placed(order, UUID.randomUUID(), 11));
        assertThat(result(order).eventType()).isEqualTo("InventoryRejected");
        assertThat(result(order).reasonCode()).isEqualTo("INSUFFICIENT_STOCK");
        assertThat(result(order).shortages()).containsExactly(new InventoryResult.Shortage("APPLE", 11, 10));
        assertThat(count("inventory_reservations")).isZero();
        assertThat(count("inventory_reservation_attempts")).isOne();
        assertThat(count("inventory_event_inbox")).isOne();
        assertThat(inventory.getStock("APPLE").availableQuantity()).isEqualTo(10);
    }

    @Test
    void unknownSkuIsRejectedWithZeroAvailability() throws Exception {
        UUID order = UUID.randomUUID();
        ObjectNode body = (ObjectNode) events.content(placed(order, UUID.randomUUID(), 2));
        ((ObjectNode) body.get("lines").get(0)).put("sku", "UNKNOWN");
        handler.handle(order.toString(), body.toString());
        assertThat(result(order).shortages()).containsExactly(new InventoryResult.Shortage("UNKNOWN", 2, 0));
        assertThat(count("inventory_reservations")).isZero();
        assertThat(inventory.getStock("APPLE").availableQuantity()).isEqualTo(10);
    }

    @Test
    void invalidEnvelopeAndKeyLeaveNoIntakeState() throws Exception {
        UUID order = UUID.randomUUID();
        String body = placed(order, UUID.randomUUID(), 2);
        assertThatThrownBy(() -> handler.handle(UUID.randomUUID().toString(), body))
                .isInstanceOf(EventProtocolException.class);
        ObjectNode invalid = (ObjectNode) events.content(body);
        invalid.put("schemaVersion", 2);
        assertThatThrownBy(() -> handler.handle(order.toString(), invalid.toString()))
                .isInstanceOf(EventProtocolException.class);
        assertEmptyIntake();
        assertThat(count("inventory_reservation_attempts")).isZero();
    }

    @Test
    void storesRequireAnOuterTransaction() throws Exception {
        UUID order = UUID.randomUUID();
        String body = placed(order, UUID.randomUUID(), 2);
        var content = events.content(body);
        assertThatThrownBy(() -> inbox.claim("test", UUID.randomUUID(), content))
                .isInstanceOf(IllegalTransactionStateException.class);
        handler.handle(order.toString(), body);
        InventoryResult result = result(order);
        assertThatThrownBy(() -> results.insert(result, events.writeResult(result)))
                .isInstanceOf(IllegalTransactionStateException.class);
        assertThat(count("inventory_event_inbox")).isOne();
    }

    @Test
    void resultInsertFailureRollsBackEveryEffect() throws Exception {
        InventoryResultStore target = AopTestUtils.getUltimateTargetObject(results);
        doThrow(new IllegalStateException("forced result failure")).when(target).insert(any(), any());
        UUID order = UUID.randomUUID();
        String body = placed(order, UUID.randomUUID(), 2);
        assertThatThrownBy(() -> handler.handle(order.toString(), body))
                .isInstanceOf(IllegalStateException.class).hasMessage("forced result failure");
        assertEmptyIntake();
        assertThat(count("inventory_reservation_attempts")).isZero();
        assertThat(count("inventory_reservations")).isZero();
        assertThat(count("inventory_reservation_lines")).isZero();
        assertThat(inventory.getStock("APPLE").availableQuantity()).isEqualTo(10);
        assertThat(inventory.getStock("BREAD").availableQuantity()).isEqualTo(10);
    }

    @Test
    void changedContentAndChangedIdentityCannotCreateAnotherResult() throws Exception {
        UUID order = UUID.randomUUID();
        UUID event = UUID.randomUUID();
        handler.handle(order.toString(), placed(order, event, 2));
        String changed = placed(order, event, 3);
        assertThatThrownBy(() -> handler.handle(order.toString(), changed))
                .isInstanceOf(EventProtocolException.class);
        String second = placed(order, UUID.randomUUID(), 2);
        assertThatThrownBy(() -> handler.handle(order.toString(), second))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThat(count("inventory_event_inbox")).isOne();
        assertThat(count("inventory_result_outbox")).isOne();
        assertThat(inventory.getStock("APPLE").availableQuantity()).isEqualTo(8);
    }

    @Test
    void matchingHttpAcceptedAndRejectedAttemptsSupplyPersistedOutcomes() throws Exception {
        UUID accepted = UUID.randomUUID();
        String success = placed(accepted, UUID.randomUUID(), 2);
        inventory.reserve(command(success));
        handler.handle(accepted.toString(), success);
        assertThat(result(accepted).eventType()).isEqualTo("InventoryReserved");
        assertThat(inventory.getStock("APPLE").availableQuantity()).isEqualTo(8);
        UUID rejected = UUID.randomUUID();
        String failure = placed(rejected, UUID.randomUUID(), 9);
        inventory.reserve(command(failure));
        jdbc.update("UPDATE stock SET available_quantity = 20 WHERE sku = 'APPLE'");
        handler.handle(rejected.toString(), failure);
        assertThat(result(rejected).shortages()).containsExactly(new InventoryResult.Shortage("APPLE", 9, 8));
        assertThat(inventory.getStock("APPLE").availableQuantity()).isEqualTo(20);
    }

    @Test
    void conflictingHistoricalAttemptRollsBackInboxClaim() throws Exception {
        UUID order = UUID.randomUUID();
        inventory.reserve(command(placed(order, UUID.randomUUID(), 2)));
        String conflict = placed(order, UUID.randomUUID(), 3);
        assertThatThrownBy(() -> handler.handle(order.toString(), conflict))
                .isInstanceOf(ReservationPayloadConflictException.class);
        assertEmptyIntake();
        assertThat(count("inventory_reservation_attempts")).isOne();
        assertThat(inventory.getStock("APPLE").availableQuantity()).isEqualTo(8);
    }

    @Test
    void releasedAttemptCannotEmitInitialSuccess() throws Exception {
        UUID order = UUID.randomUUID();
        String body = placed(order, UUID.randomUUID(), 2);
        inventory.reserve(command(body));
        inventory.release(order);
        assertThatThrownBy(() -> handler.handle(order.toString(), body))
                .isInstanceOf(EventProtocolException.class).hasMessage("RESERVATION_RELEASED");
        assertEmptyIntake();
        assertThat(inventory.getStock("APPLE").availableQuantity()).isEqualTo(10);
    }

    @Test
    void reorderedHttpReplayPreservesEventOrderButInboxArrayOrderIsSignificant() throws Exception {
        UUID order = UUID.randomUUID();
        String body = placed(order, UUID.randomUUID(), 2);
        inventory.reserve(command(body));
        ObjectNode reordered = (ObjectNode) events.content(body);
        ArrayNode lines = (ArrayNode) reordered.get("lines");
        var first = lines.remove(0);
        lines.add(first);
        handler.handle(order.toString(), reordered.toString());
        assertThat(result(order).lines().stream().map(InventoryResult.Line::sku).toList())
                .containsExactlyElementsOf(events.readOrderPlaced(order.toString(), reordered.toString())
                        .lines().stream().map(line -> line.sku()).toList());
        assertThatThrownBy(() -> handler.handle(order.toString(), body)).isInstanceOf(EventProtocolException.class);
        assertThat(inventory.getStock("APPLE").availableQuantity()).isEqualTo(8);
    }

    @Test
    void concurrentIdenticalIntakeHasOneWinner() throws Exception {
        UUID order = UUID.randomUUID();
        String body = placed(order, UUID.randomUUID(), 2);
        CyclicBarrier start = new CyclicBarrier(2);
        var first = workers.submit(() -> { start.await(5, SECONDS); return handler.handle(order.toString(), body); });
        var second = workers.submit(() -> { start.await(5, SECONDS); return handler.handle(order.toString(), body); });
        assertThat(List.of(first.get(10, SECONDS), second.get(10, SECONDS)))
                .containsExactlyInAnyOrder(ProcessingOutcome.APPLIED, ProcessingOutcome.DUPLICATE);
        assertThat(count("inventory_event_inbox")).isOne();
        assertThat(count("inventory_result_outbox")).isOne();
        assertThat(inventory.getStock("APPLE").availableQuantity()).isEqualTo(8);
    }

    @Test
    void distinctOrdersCompeteWithoutOverselling() throws Exception {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        String firstBody = placed(a, UUID.randomUUID(), 6);
        String secondBody = placed(b, UUID.randomUUID(), 6);
        CyclicBarrier start = new CyclicBarrier(2);
        var first = workers.submit(() -> { start.await(5, SECONDS); return handler.handle(a.toString(), firstBody); });
        var second = workers.submit(() -> { start.await(5, SECONDS); return handler.handle(b.toString(), secondBody); });
        assertThat(first.get(10, SECONDS)).isEqualTo(ProcessingOutcome.APPLIED);
        assertThat(second.get(10, SECONDS)).isEqualTo(ProcessingOutcome.APPLIED);
        assertThat(List.of(result(a).eventType(), result(b).eventType()))
                .containsExactlyInAnyOrder("InventoryReserved", "InventoryRejected");
        assertThat(count("inventory_result_outbox")).isEqualTo(2);
        assertThat(inventory.getStock("APPLE").availableQuantity()).isEqualTo(4);
    }

    @Test
    void replayRefreshesPreviouslyManagedReservationAfterReleaseWins() throws Exception {
        UUID order = UUID.randomUUID();
        String body = placed(order, UUID.randomUUID(), 2);
        inventory.reserve(command(body));
        assertThatThrownBy(() -> new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
            assertThat(inventory.getReservation(order).status()).isEqualTo(ReservationStatus.RESERVED);
            try {
                workers.submit(() -> inventory.release(order)).get(10, SECONDS);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            handler.handle(order.toString(), body);
        })).isInstanceOf(EventProtocolException.class).hasMessage("RESERVATION_RELEASED");
        assertEmptyIntake();
    }

    @Test
    void intakeWaitsForConcurrentHttpReleaseThenRejects() throws Exception {
        UUID order = UUID.randomUUID();
        String body = placed(order, UUID.randomUUID(), 2);
        inventory.reserve(command(body));
        CountDownLatch released = new CountDownLatch(1);
        CountDownLatch commit = new CountDownLatch(1);
        var release = workers.submit(() -> new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
            inventory.release(order);
            released.countDown();
            await(commit);
        }));
        assertThat(released.await(5, SECONDS)).isTrue();
        var intake = workers.submit(() -> handler.handle(order.toString(), body));
        try {
            awaitBlockedTransaction();
            assertThat(intake.isDone()).isFalse();
        } finally {
            commit.countDown();
        }
        release.get(10, SECONDS);
        assertThatThrownBy(() -> intake.get(10, SECONDS)).hasCauseInstanceOf(EventProtocolException.class);
        assertEmptyIntake();
        assertThat(inventory.getStock("APPLE").availableQuantity()).isEqualTo(10);
    }

    @Test
    void intakeHoldsReservationLockUntilResultCommit() throws Exception {
        UUID order = UUID.randomUUID();
        String body = placed(order, UUID.randomUUID(), 2);
        inventory.reserve(command(body));
        CountDownLatch creatingResult = new CountDownLatch(1);
        CountDownLatch commit = new CountDownLatch(1);
        InventoryResultStore target = AopTestUtils.getUltimateTargetObject(results);
        doAnswer(call -> {
            creatingResult.countDown();
            await(commit);
            return call.callRealMethod();
        }).when(target).insert(any(), any());
        var intake = workers.submit(() -> handler.handle(order.toString(), body));
        assertThat(creatingResult.await(5, SECONDS)).isTrue();
        var release = workers.submit(() -> inventory.release(order));
        try {
            awaitBlockedTransaction();
            assertThat(release.isDone()).isFalse();
            assertEmptyIntake();
        } finally {
            commit.countDown();
        }
        assertThat(intake.get(10, SECONDS)).isEqualTo(ProcessingOutcome.APPLIED);
        assertThat(release.get(10, SECONDS).status()).isEqualTo(ReservationStatus.RELEASED);
        assertThat(result(order).eventType()).isEqualTo("InventoryReserved");
        // Release linearizes after intake; post-intake manual release is unsupported, not compensation.
        assertThat(count("inventory_event_inbox")).isOne();
        assertThat(count("inventory_result_outbox")).isOne();
    }

    private void awaitBlockedTransaction() throws Exception {
        long deadline = System.nanoTime() + SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (jdbc.queryForObject("SELECT count(*) FROM pg_stat_activity "
                    + "WHERE datname = current_database() AND cardinality(pg_blocking_pids(pid)) > 0", Integer.class) > 0) return;
            Thread.sleep(10);
        }
        throw new AssertionError("Expected a PostgreSQL lock waiter");
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, SECONDS)) throw new AssertionError("Timed out waiting for commit");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private void assertEmptyIntake() {
        assertThat(count("inventory_event_inbox")).isZero();
        assertThat(count("inventory_result_outbox")).isZero();
    }

    private InventoryResult result(UUID order) {
        return events.readResult(order.toString(), jdbc.queryForObject(
                "SELECT payload FROM inventory_result_outbox WHERE order_id = ?", String.class, order));
    }

    private ReserveInventoryCommand command(String body) {
        var event = events.readOrderPlaced(events.content(body).get("orderId").asText(), body);
        return new ReserveInventoryCommand(event.orderId(), event.lines().stream()
                .map(line -> new ReserveInventoryCommand.Line(line.sku(), line.quantity())).toList());
    }

    private String placed(UUID orderId, UUID eventId, int quantity) throws Exception {
        try (var input = getClass().getResourceAsStream("/contracts/events/orders/v1/order-placed.json")) {
            ObjectNode node = (ObjectNode) events.content(new String(input.readAllBytes(), StandardCharsets.UTF_8));
            node.put("orderId", orderId.toString());
            node.put("eventId", eventId.toString());
            node.get("lines").forEach(line -> {
                String sku = line.get("sku").asText();
                if (sku.equals("SKU-A")) ((ObjectNode) line).put("sku", "APPLE");
                if (sku.equals("SKU-B")) ((ObjectNode) line).put("sku", "BREAD");
                if (line.get("sku").asText().equals("APPLE")) ((ObjectNode) line).put("quantity", quantity);
            });
            return node.toString();
        }
    }

    private long count(String table) {
        if (!Set.of("inventory_event_inbox", "inventory_result_outbox", "inventory_reservation_attempts",
                "inventory_reservations", "inventory_reservation_lines").contains(table)) throw new IllegalArgumentException();
        return jdbc.queryForObject("SELECT count(*) FROM " + table, Long.class);
    }
}
