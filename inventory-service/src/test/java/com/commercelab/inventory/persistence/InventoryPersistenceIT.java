package com.commercelab.inventory.persistence;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.commercelab.inventory.AbstractPostgresIntegrationTest;
import com.commercelab.inventory.domain.Reservation;
import com.commercelab.inventory.domain.ReservationLine;
import com.commercelab.inventory.domain.ReservationStatus;
import com.commercelab.inventory.domain.StockItem;
import com.commercelab.inventory.repository.ReservationRepository;
import com.commercelab.inventory.repository.StockRepository;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest
class InventoryPersistenceIT extends AbstractPostgresIntegrationTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private StockRepository stockRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @BeforeEach
    void resetDatabase() {
        jdbcTemplate.execute(
                "TRUNCATE TABLE inventory_reservation_lines, inventory_reservations, stock CASCADE");
        jdbcTemplate.update(
                "INSERT INTO stock (sku, available_quantity) VALUES ('SKU-1', 2), ('SKU-2', 3)");
    }

    @Test
    void reservationRoundTripPreservesRequestOrderAndReleaseState() {
        UUID orderId = UUID.randomUUID();
        Instant reservedAt = Instant.parse("2026-09-07T10:15:30Z");
        Instant releasedAt = Instant.parse("2026-09-07T10:16:30Z");
        Reservation reservation = Reservation.reserve(orderId, List.of(
                new ReservationLine("SKU-2", 2),
                new ReservationLine("SKU-1", 1)), reservedAt);

        reservationRepository.add(reservation);

        assertThat(reservationRepository.existsByOrderId(orderId)).isTrue();
        assertThat(reservationRepository.findByOrderId(orderId)).contains(reservation);
        assertThat(reservationRepository.lockByOrderId(orderId)).contains(reservation);

        reservationRepository.markReleased(orderId, releasedAt);

        Reservation released = reservationRepository.findByOrderId(orderId).orElseThrow();
        assertThat(released.status()).isEqualTo(ReservationStatus.RELEASED);
        assertThat(released.releasedAt()).isEqualTo(releasedAt);
        assertThat(released.lines()).extracting(ReservationLine::sku)
                .containsExactly("SKU-2", "SKU-1");
    }

    @Test
    void stockReadsUpdatesAndLocksRequestedRowsInAscendingSkuOrder() {
        assertThat(stockRepository.findBySku("SKU-1"))
                .contains(new StockItem("SKU-1", 2));

        List<StockItem> locked = stockRepository.lockBySkus(List.of("SKU-2", "SKU-1"));
        assertThat(locked).containsExactly(
                new StockItem("SKU-1", 2),
                new StockItem("SKU-2", 3));

        stockRepository.updateAll(List.of(
                new StockItem("SKU-1", 1),
                new StockItem("SKU-2", 0)));

        assertThat(stockRepository.findBySku("SKU-1"))
                .contains(new StockItem("SKU-1", 1));
        assertThat(stockRepository.findBySku("SKU-2"))
                .contains(new StockItem("SKU-2", 0));
    }

    @Test
    void stockUpdateRejectsAMissingPreviouslyLockedRow() {
        assertThatThrownBy(() -> stockRepository.updateAll(List.of(new StockItem("SKU-404", 1))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SKU-404");
    }

    @Test
    void pessimisticStockLockBlocksAConcurrentTransaction() throws Exception {
        TransactionTemplate transactions = new TransactionTemplate(transactionManager);
        CountDownLatch firstLockAcquired = new CountDownLatch(1);
        CountDownLatch releaseFirstTransaction = new CountDownLatch(1);
        CountDownLatch secondLockAttempted = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<?> first = executor.submit(() -> transactions.executeWithoutResult(status -> {
                stockRepository.lockBySkus(List.of("SKU-1"));
                firstLockAcquired.countDown();
                await(releaseFirstTransaction);
            }));
            assertThat(firstLockAcquired.await(5, SECONDS)).isTrue();

            Future<List<StockItem>> second = executor.submit(() -> transactions.execute(status -> {
                secondLockAttempted.countDown();
                return stockRepository.lockBySkus(List.of("SKU-1"));
            }));
            assertThat(secondLockAttempted.await(5, SECONDS)).isTrue();

            Thread.sleep(200);
            assertThat(second.isDone()).isFalse();

            releaseFirstTransaction.countDown();
            assertThat(first.get(5, SECONDS)).isNull();
            assertThat(second.get(5, SECONDS))
                    .containsExactly(new StockItem("SKU-1", 2));
        } finally {
            releaseFirstTransaction.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, SECONDS)).isTrue();
        }
    }

    @Test
    void reservationLockWaiterSeesStateCommittedBeforeItAcquiresTheLock() throws Exception {
        UUID orderId = UUID.randomUUID();
        Instant releasedAt = Instant.parse("2026-09-07T10:16:30Z");
        reservationRepository.add(Reservation.reserve(orderId, List.of(
                new ReservationLine("SKU-2", 2),
                new ReservationLine("SKU-1", 1)), Instant.parse("2026-09-07T10:15:30Z")));

        TransactionTemplate transactions = new TransactionTemplate(transactionManager);
        CountDownLatch firstLockAcquired = new CountDownLatch(1);
        CountDownLatch releaseFirstTransaction = new CountDownLatch(1);
        CountDownLatch secondTransactionStarted = new CountDownLatch(1);
        AtomicInteger secondBackendPid = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<?> first = executor.submit(() -> transactions.executeWithoutResult(status -> {
                reservationRepository.lockByOrderId(orderId).orElseThrow();
                firstLockAcquired.countDown();
                await(releaseFirstTransaction);
                reservationRepository.markReleased(orderId, releasedAt);
            }));
            assertThat(firstLockAcquired.await(5, SECONDS)).isTrue();

            Future<Reservation> second = executor.submit(() -> transactions.execute(status -> {
                secondBackendPid.set(jdbcTemplate.queryForObject(
                        "SELECT pg_backend_pid()", Integer.class));
                secondTransactionStarted.countDown();
                return reservationRepository.lockByOrderId(orderId).orElseThrow();
            }));
            assertThat(secondTransactionStarted.await(5, SECONDS)).isTrue();
            awaitDatabaseLock(secondBackendPid.get());
            assertThat(second.isDone()).isFalse();

            releaseFirstTransaction.countDown();
            assertThat(first.get(5, SECONDS)).isNull();

            Reservation waiterSnapshot = second.get(5, SECONDS);
            assertThat(waiterSnapshot.status()).isEqualTo(ReservationStatus.RELEASED);
            assertThat(waiterSnapshot.releasedAt()).isEqualTo(releasedAt);
            assertThat(waiterSnapshot.lines()).extracting(ReservationLine::sku)
                    .containsExactly("SKU-2", "SKU-1");
        } finally {
            releaseFirstTransaction.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, SECONDS)).isTrue();
        }
    }

    private void awaitDatabaseLock(int backendPid) throws InterruptedException {
        long deadline = System.nanoTime() + SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            Boolean waiting = jdbcTemplate.queryForObject("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_stat_activity
                        WHERE pid = ? AND wait_event_type = 'Lock'
                    )
                    """, Boolean.class, backendPid);
            if (Boolean.TRUE.equals(waiting)) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("PostgreSQL backend did not wait for the reservation lock");
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, SECONDS)) {
                throw new IllegalStateException("timed out waiting to release database lock");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting to release database lock", exception);
        }
    }
}
