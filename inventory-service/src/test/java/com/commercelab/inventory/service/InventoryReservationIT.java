package com.commercelab.inventory.service;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.commercelab.inventory.AbstractPostgresIntegrationTest;
import com.commercelab.inventory.domain.Availability;
import com.commercelab.inventory.domain.Reservation;
import com.commercelab.inventory.domain.ReservationAlreadyExistsException;
import com.commercelab.inventory.domain.ReservationLine;
import com.commercelab.inventory.domain.ReservationNotFoundException;
import com.commercelab.inventory.domain.ReservationStatus;
import com.commercelab.inventory.domain.StockItem;
import com.commercelab.inventory.domain.StockNotFoundException;
import com.commercelab.inventory.domain.StockUnavailableException;
import com.commercelab.inventory.repository.StockRepository;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest
class InventoryReservationIT extends AbstractPostgresIntegrationTest {

    @Autowired
    private InventoryService service;

    @Autowired
    private StockRepository stockRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private ExecutorService executor;

    @BeforeEach
    void resetDatabase() {
        jdbcTemplate.execute(
                "ALTER TABLE inventory_reservations DROP CONSTRAINT IF EXISTS task_4_other_integrity");
        jdbcTemplate.execute(
                "TRUNCATE TABLE inventory_reservation_lines, inventory_reservations, stock CASCADE");
        executor = Executors.newFixedThreadPool(3);
    }

    @AfterEach
    void stopExecutor() throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(5, SECONDS)).isTrue();
    }

    @Test
    void reservesMultipleSkusAtomicallyAndPreservesRequestOrder() {
        insertStock("SKU-A", 5);
        insertStock("SKU-B", 3);
        UUID orderId = UUID.randomUUID();

        Reservation reservation = service.reserve(command(orderId,
                line("SKU-B", 2), line("SKU-A", 1)));

        assertThat(reservation.orderId()).isEqualTo(orderId);
        assertThat(reservation.lines()).containsExactly(
                new ReservationLine("SKU-B", 2),
                new ReservationLine("SKU-A", 1));
        assertThat(available("SKU-A")).isEqualTo(4);
        assertThat(available("SKU-B")).isEqualTo(1);
        assertThat(reservationCount(orderId)).isOne();
    }

    @Test
    void insufficientSecondSkuRollsBackEveryChange() {
        insertStock("SKU-A", 5);
        insertStock("SKU-B", 1);
        UUID orderId = UUID.randomUUID();

        assertThatThrownBy(() -> service.reserve(command(orderId,
                line("SKU-A", 2), line("SKU-B", 2))))
                .isInstanceOf(StockUnavailableException.class);

        assertThat(available("SKU-A")).isEqualTo(5);
        assertThat(available("SKU-B")).isEqualTo(1);
        assertThat(reservationCount(orderId)).isZero();
    }

    @Test
    void reportsEveryUnavailableSkuInRequestOrderAndUnknownAvailabilityAsZero() {
        insertStock("SKU-A", 1);
        insertStock("SKU-B", 2);
        insertStock("SKU-OK", 5);

        assertThatThrownBy(() -> service.reserve(command(UUID.randomUUID(),
                line("SKU-OK", 5),
                line("SKU-B", 3),
                line("SKU-UNKNOWN", 4),
                line("SKU-A", 2))))
                .isInstanceOfSatisfying(StockUnavailableException.class, exception -> {
                    assertThat(exception.unavailableSkus()).containsExactly(
                            Map.entry("SKU-B", new Availability(3, 2)),
                            Map.entry("SKU-UNKNOWN", new Availability(4, 0)),
                            Map.entry("SKU-A", new Availability(2, 1)));
                    assertThatThrownBy(() -> exception.unavailableSkus()
                            .put("SKU-X", new Availability(1, 0)))
                            .isInstanceOf(UnsupportedOperationException.class);
                });

        assertThat(available("SKU-A")).isEqualTo(1);
        assertThat(available("SKU-B")).isEqualTo(2);
        assertThat(available("SKU-OK")).isEqualTo(5);
        assertThat(reservationCount()).isZero();
    }

    @Test
    void existingOrderIdIsAlwaysAConflictWithoutChangingStockAgain() {
        insertStock("SKU-A", 3);
        UUID orderId = UUID.randomUUID();
        service.reserve(command(orderId, line("SKU-A", 1)));

        assertThatThrownBy(() -> service.reserve(command(orderId, line("SKU-A", 2))))
                .isInstanceOf(ReservationAlreadyExistsException.class);

        assertThat(available("SKU-A")).isEqualTo(2);
        assertThat(reservationCount(orderId)).isOne();
    }

    @Test
    void concurrentOrdersCannotReserveTheLastUnitTwice() throws Exception {
        insertStock("SKU-LAST", 1);
        CyclicBarrier start = new CyclicBarrier(2);

        Future<Outcome> first = submitReserve(
                start, UUID.randomUUID(), line("SKU-LAST", 1));
        Future<Outcome> second = submitReserve(
                start, UUID.randomUUID(), line("SKU-LAST", 1));

        assertThat(List.of(first.get(10, SECONDS), second.get(10, SECONDS)))
                .containsExactlyInAnyOrder(Outcome.RESERVED, Outcome.UNAVAILABLE);
        assertThat(available("SKU-LAST")).isZero();
        assertThat(reservationCount()).isOne();
    }

    @Test
    void reversedSkuRequestsCompleteWithoutDeadlock() throws Exception {
        insertStock("SKU-A", 2);
        insertStock("SKU-B", 2);
        CyclicBarrier start = new CyclicBarrier(2);

        Future<Outcome> first = submitReserve(start, UUID.randomUUID(),
                line("SKU-A", 1), line("SKU-B", 1));
        Future<Outcome> second = submitReserve(start, UUID.randomUUID(),
                line("SKU-B", 1), line("SKU-A", 1));

        assertThat(List.of(first.get(10, SECONDS), second.get(10, SECONDS)))
                .containsOnly(Outcome.RESERVED);
        assertThat(available("SKU-A")).isZero();
        assertThat(available("SKU-B")).isZero();
        assertThat(reservationCount()).isEqualTo(2);
    }

    @Test
    void concurrentSameOrderPrimaryKeyViolationBecomesDuplicateConflictAndRollsBackStock()
            throws Exception {
        insertStock("SKU-A", 2);
        CountDownLatch stockLockHeld = new CountDownLatch(1);
        CountDownLatch releaseStockLock = new CountDownLatch(1);
        TransactionTemplate transactions = new TransactionTemplate(transactionManager);
        Future<?> blocker = executor.submit(() -> transactions.executeWithoutResult(status -> {
            stockRepository.lockBySkus(List.of("SKU-A"));
            stockLockHeld.countDown();
            await(releaseStockLock);
        }));
        assertThat(stockLockHeld.await(5, SECONDS)).isTrue();

        UUID orderId = UUID.randomUUID();
        CyclicBarrier start = new CyclicBarrier(2);
        Future<Outcome> first = submitReserve(start, orderId, line("SKU-A", 1));
        Future<Outcome> second = submitReserve(start, orderId, line("SKU-A", 1));

        try {
            awaitBlockedTransactions(2);
        } finally {
            releaseStockLock.countDown();
        }

        assertThat(blocker.get(10, SECONDS)).isNull();
        assertThat(List.of(first.get(10, SECONDS), second.get(10, SECONDS)))
                .containsExactlyInAnyOrder(Outcome.RESERVED, Outcome.DUPLICATE);
        assertThat(available("SKU-A")).isOne();
        assertThat(reservationCount(orderId)).isOne();
    }

    @Test
    void unrelatedIntegrityViolationPropagatesAndRollsBackStock() {
        insertStock("SKU-A", 2);
        UUID orderId = UUID.randomUUID();
        jdbcTemplate.execute("""
                ALTER TABLE inventory_reservations
                ADD CONSTRAINT task_4_other_integrity CHECK (status <> 'RESERVED')
                """);

        try {
            assertThatThrownBy(() -> service.reserve(command(orderId, line("SKU-A", 1))))
                    .isInstanceOf(ConstraintViolationException.class)
                    .isNotInstanceOf(ReservationAlreadyExistsException.class);
        } finally {
            jdbcTemplate.execute(
                    "ALTER TABLE inventory_reservations DROP CONSTRAINT task_4_other_integrity");
        }

        assertThat(available("SKU-A")).isEqualTo(2);
        assertThat(reservationCount(orderId)).isZero();
    }

    @Test
    void repeatedReleaseRestoresStockOnce() {
        insertStock("SKU-1", 5);
        UUID orderId = reserve("SKU-1", 2);

        assertThat(service.release(orderId).status()).isEqualTo(ReservationStatus.RELEASED);
        assertThat(service.release(orderId).status()).isEqualTo(ReservationStatus.RELEASED);

        assertThat(available("SKU-1")).isEqualTo(5);
    }

    @Test
    void concurrentReleaseRestoresStockOnce() throws Exception {
        insertStock("SKU-1", 5);
        UUID orderId = reserve("SKU-1", 2);
        CyclicBarrier start = new CyclicBarrier(2);

        Future<Reservation> first = submitRelease(start, orderId);
        Future<Reservation> second = submitRelease(start, orderId);

        assertThat(first.get(10, SECONDS).status()).isEqualTo(ReservationStatus.RELEASED);
        assertThat(second.get(10, SECONDS).status()).isEqualTo(ReservationStatus.RELEASED);
        assertThat(available("SKU-1")).isEqualTo(5);
    }

    @Test
    void releaseUnknownReservationThrowsTypedNotFound() {
        UUID orderId = UUID.randomUUID();

        assertThatThrownBy(() -> service.release(orderId))
                .isInstanceOf(ReservationNotFoundException.class)
                .hasMessageContaining(orderId.toString());
    }

    @Test
    void retrievesReservationBeforeAndAfterRelease() {
        insertStock("SKU-1", 5);
        UUID orderId = reserve("SKU-1", 2);

        assertThat(service.getReservation(orderId).status()).isEqualTo(ReservationStatus.RESERVED);

        service.release(orderId);

        assertThat(service.getReservation(orderId).status()).isEqualTo(ReservationStatus.RELEASED);
    }

    @Test
    void retrievesStock() {
        insertStock("SKU-1", 5);

        assertThat(service.getStock("SKU-1")).isEqualTo(new StockItem("SKU-1", 5));
    }

    @Test
    void unknownReservationReadThrowsTypedNotFound() {
        UUID orderId = UUID.randomUUID();

        assertThatThrownBy(() -> service.getReservation(orderId))
                .isInstanceOf(ReservationNotFoundException.class)
                .hasMessageContaining(orderId.toString());
    }

    @Test
    void unknownStockReadThrowsTypedNotFound() {
        assertThatThrownBy(() -> service.getStock("SKU-UNKNOWN"))
                .isInstanceOf(StockNotFoundException.class)
                .hasMessageContaining("SKU-UNKNOWN");
    }

    private Future<Outcome> submitReserve(
            CyclicBarrier start, UUID orderId, ReserveInventoryCommand.Line... lines) {
        return executor.submit(() -> {
            await(start);
            try {
                service.reserve(command(orderId, lines));
                return Outcome.RESERVED;
            } catch (StockUnavailableException exception) {
                return Outcome.UNAVAILABLE;
            } catch (ReservationAlreadyExistsException exception) {
                return Outcome.DUPLICATE;
            }
        });
    }

    private Future<Reservation> submitRelease(CyclicBarrier start, UUID orderId) {
        return executor.submit(() -> {
            await(start);
            return service.release(orderId);
        });
    }

    private UUID reserve(String sku, int quantity) {
        UUID orderId = UUID.randomUUID();
        service.reserve(command(orderId, line(sku, quantity)));
        return orderId;
    }

    private void awaitBlockedTransactions(int expected) throws InterruptedException {
        long deadline = System.nanoTime() + SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            Integer blocked = jdbcTemplate.queryForObject("""
                    SELECT count(*)
                    FROM pg_stat_activity
                    WHERE cardinality(pg_blocking_pids(pid)) > 0
                    """, Integer.class);
            if (blocked != null && blocked >= expected) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("PostgreSQL did not report " + expected + " blocked transactions");
    }

    private void insertStock(String sku, int quantity) {
        jdbcTemplate.update(
                "INSERT INTO stock (sku, available_quantity) VALUES (?, ?)", sku, quantity);
    }

    private int available(String sku) {
        return jdbcTemplate.queryForObject(
                "SELECT available_quantity FROM stock WHERE sku = ?", Integer.class, sku);
    }

    private int reservationCount() {
        return jdbcTemplate.queryForObject(
                "SELECT count(*) FROM inventory_reservations", Integer.class);
    }

    private int reservationCount(UUID orderId) {
        return jdbcTemplate.queryForObject(
                "SELECT count(*) FROM inventory_reservations WHERE order_id = ?",
                Integer.class, orderId);
    }

    private static ReserveInventoryCommand command(
            UUID orderId, ReserveInventoryCommand.Line... lines) {
        return new ReserveInventoryCommand(orderId, List.of(lines));
    }

    private static ReserveInventoryCommand.Line line(String sku, int quantity) {
        return new ReserveInventoryCommand.Line(sku, quantity);
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await(5, SECONDS);
        } catch (Exception exception) {
            throw new IllegalStateException("failed to synchronize concurrent reservation", exception);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, SECONDS)) {
                throw new IllegalStateException("timed out waiting to release stock lock");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting to release stock lock", exception);
        }
    }

    private enum Outcome {
        RESERVED,
        UNAVAILABLE,
        DUPLICATE
    }
}
