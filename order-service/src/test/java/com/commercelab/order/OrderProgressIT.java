package com.commercelab.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

import com.commercelab.order.domain.OrderNotFoundException;
import com.commercelab.order.domain.OrderStatus;
import com.commercelab.order.inventory.InventoryLine;
import com.commercelab.order.inventory.InventoryOutcome;
import com.commercelab.order.inventory.StockShortage;
import com.commercelab.order.persistence.OrderJpaRepository;
import com.commercelab.order.service.OrderCreationService;
import com.commercelab.order.service.OrderProgressService;
import com.commercelab.order.service.PlaceOrderCommand;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = "spring.jpa.properties.hibernate.query.fail_on_pagination_over_collection_fetch=true")
@AutoConfigureMockMvc
class OrderProgressIT extends AbstractPostgresIntegrationTest {
    static final Instant NOW = Instant.parse("2026-09-19T12:00:00Z");
    @TestConfiguration
    static class TimeConfig {
        @Bean @Primary Clock testClock() { return Clock.fixed(NOW, ZoneOffset.UTC); }
    }

    @Autowired OrderCreationService creation;
    @Autowired OrderProgressService progress;
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mvc;
    @Autowired jakarta.persistence.EntityManager entityManager;
    @Autowired org.springframework.transaction.PlatformTransactionManager transactionManager;
    @SpyBean OrderJpaRepository orders;

    @BeforeEach
    void clear() { jdbc.execute("TRUNCATE order_requests, order_lines, orders CASCADE"); }

    UUID pending() {
        return creation.createOrReplay(UUID.randomUUID().toString(), new PlaceOrderCommand("cust", "EUR",
                List.of(new PlaceOrderCommand.Line("B", 2, BigDecimal.ONE),
                        new PlaceOrderCommand.Line("A", 1, BigDecimal.TEN))), "origin").order().id();
    }

    InventoryOutcome reserved(UUID id) {
        return new InventoryOutcome.Reserved(id, List.of(new InventoryLine("A", 1), new InventoryLine("B", 2)));
    }

    InventoryOutcome rejected() { return new InventoryOutcome.Rejected(Map.of("B", new StockShortage(2, 0))); }

    @Test
    void confirmsAndRejectsWithoutRegressingTerminalResults() throws Exception {
        UUID confirmed = pending();
        progress.defer(confirmed, "TIMEOUT", NOW.plusSeconds(20));
        assertThat(progress.apply(confirmed, reserved(confirmed)).status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(progress.apply(confirmed, rejected()).status()).isEqualTo(OrderStatus.CONFIRMED);
        progress.defer(confirmed, "STALE", NOW);
        progress.block(confirmed, "STALE");
        assertThat(progress.load(confirmed).order().recoveryIssue()).isNull();
        assertThat(progress.load(confirmed).attemptCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT next_attempt_at FROM orders WHERE id = ?", Instant.class, confirmed)).isNull();
        UUID rejected = pending();
        assertThat(progress.apply(rejected, rejected()).rejectionReason()).isEqualTo("STOCK_UNAVAILABLE");
        assertThat(progress.apply(rejected, reserved(rejected)).status()).isEqualTo(OrderStatus.REJECTED);
        mvc.perform(get("/api/v1/orders/" + rejected)).andExpect(jsonPath("$.rejectionReason").value("STOCK_UNAVAILABLE"));
    }

    @Test
    void initialEligibilityUsesClockAndDueIdsAreBoundedOrderedAndExcludeHistoricalOrBlocked() {
        UUID first = pending();
        UUID second = pending();
        UUID historical = pending();
        UUID blocked = pending();
        jdbc.update("UPDATE orders SET status = 'PLACED' WHERE id = ?", historical);
        progress.block(blocked, "PAYLOAD_CONFLICT");
        assertThat(progress.findDueIds(NOW.plusSeconds(4), 20)).isEmpty();
        List<UUID> sorted = jdbc.queryForList("SELECT id FROM orders WHERE id IN (?, ?) ORDER BY id", UUID.class, first, second);
        assertThat(progress.findDueIds(NOW.plusSeconds(5), 1)).containsExactly(sorted.getFirst());
        assertThat(progress.findDueIds(NOW.plusSeconds(5), 20)).containsExactlyElementsOf(sorted);
        progress.defer(first, "TIMEOUT", NOW.plusSeconds(10));
        assertThat(progress.findDueIds(NOW.plusSeconds(10), 20)).containsExactly(second, first);
        assertThat(progress.apply(historical, reserved(historical)).status()).isEqualTo(OrderStatus.PLACED);
        progress.defer(historical, "STALE", NOW);
        progress.block(historical, "STALE");
        assertThat(progress.load(historical).attemptCount()).isZero();
        assertThatThrownBy(() -> progress.findDueIds(NOW, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void deferAndBlockSurviveNewEntityManagersAndExposeOnlyStableIssue() throws Exception {
        UUID id = pending();
        progress.defer(id, "TIMEOUT", NOW.plusSeconds(40));
        progress.defer(id, "CIRCUIT_OPEN", NOW.plusSeconds(60));
        // Each public call closes its transaction and entity manager before the next read.
        assertThat(progress.load(id).attemptCount()).isEqualTo(2);
        var state = jdbc.queryForMap("SELECT last_failure_code, last_attempt_at, next_attempt_at FROM orders WHERE id = ?", id);
        assertThat(state.get("last_failure_code")).isEqualTo("CIRCUIT_OPEN");
        assertThat(((java.sql.Timestamp) state.get("last_attempt_at")).toInstant()).isEqualTo(NOW);
        assertThat(((java.sql.Timestamp) state.get("next_attempt_at")).toInstant()).isEqualTo(NOW.plusSeconds(60));
        assertThat(progress.load(id).correlationId()).isEqualTo("origin");
        assertThat(progress.load(id).order().lines()).extracting(line -> line.sku()).containsExactly("B", "A");
        assertThat(progress.findDueIds(NOW.plusSeconds(59), 20)).isEmpty();
        assertThat(progress.findDueIds(NOW.plusSeconds(60), 20)).containsExactly(id);
        progress.block(id, "PAYLOAD_CONFLICT");
        progress.defer(id, "STALE", NOW);
        assertThat(progress.load(id).order().status()).isEqualTo(OrderStatus.PENDING_INVENTORY);
        assertThat(progress.load(id).order().recoveryIssue()).isEqualTo("PAYLOAD_CONFLICT");
        assertThat(progress.load(id).attemptCount()).isEqualTo(3);
        assertThat(progress.findDueIds(NOW.plusSeconds(600), 20)).isEmpty();
        mvc.perform(get("/api/v1/orders/" + id)).andExpect(jsonPath("$.recoveryIssue").value("PAYLOAD_CONFLICT"));
        assertThatThrownBy(() -> progress.load(UUID.randomUUID())).isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void missingAndReleasedCannotEstablishABusinessResult() {
        UUID id = pending();
        assertThat(progress.apply(id, new InventoryOutcome.Missing()).status()).isEqualTo(OrderStatus.PENDING_INVENTORY);
        assertThat(progress.apply(id, new InventoryOutcome.Released(id, List.of())).status()).isEqualTo(OrderStatus.PENDING_INVENTORY);
    }

    @Test
    void nullableOrAbsentLegacyCorrelationIsReadable() {
        UUID id = pending();
        jdbc.update("UPDATE order_requests SET correlation_id = NULL WHERE order_id = ?", id);
        assertThat(progress.load(id).correlationId()).isNull();
        jdbc.update("DELETE FROM order_requests WHERE order_id = ?", id);
        assertThat(progress.load(id).correlationId()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void staleMetadataWriteCannotRegressConcurrentFinalization(boolean block) throws Exception {
        UUID id = pending();
        CountDownLatch loaded = new CountDownLatch(1);
        CountDownLatch finalized = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        List<Long> transactions = new CopyOnWriteArrayList<>();
        doAnswer(call -> {
            var result = selectOrder(id);
            transactions.add(jdbc.queryForObject("SELECT txid_current()", Long.class));
            if (reads.incrementAndGet() == 1) {
                loaded.countDown();
                assertThat(finalized.await(10, TimeUnit.SECONDS)).isTrue();
            }
            return result;
        }).when(orders).findByIdWithLines(id);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var stale = executor.submit(() -> {
                if (block) progress.block(id, "PAYLOAD_CONFLICT");
                else progress.defer(id, "TIMEOUT", NOW.plusSeconds(30));
            });
            try {
                assertThat(loaded.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(progress.apply(id, reserved(id)).status()).isEqualTo(OrderStatus.CONFIRMED);
            } finally {
                finalized.countDown();
            }
            stale.get(20, TimeUnit.SECONDS);
        }
        assertThat(transactions).hasSize(3).doesNotHaveDuplicates();
        var result = progress.load(id);
        assertThat(result.order().status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(result.order().recoveryIssue()).isNull();
        assertThat(result.attemptCount()).isZero();
        assertThat(jdbc.queryForObject("SELECT version FROM orders WHERE id = ?", Long.class, id)).isEqualTo(1L);
    }

    @Test
    void rejectsUnstableFailureTextWithoutPersistingIt() {
        UUID id = pending();
        assertThatThrownBy(() -> progress.block(id, "remote body: secret"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> progress.defer(id, "x".repeat(65), NOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(progress.load(id).attemptCount()).isZero();
    }

    private java.util.Optional<com.commercelab.order.persistence.OrderEntity> selectOrder(UUID id) {
        // Repository interface spies cannot callRealMethod; execute the real fetch in the caller's transaction.
        return entityManager.createQuery("select o from OrderEntity o left join fetch o.lines where o.id = :id",
                com.commercelab.order.persistence.OrderEntity.class).setParameter("id", id)
                .getResultStream().findFirst();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void exhaustedWriteRetriesReloadTerminalWinnerOrPropagateContention(boolean terminalWinner) {
        UUID id = pending();
        AtomicInteger reads = new AtomicInteger();
        var concurrent = new org.springframework.transaction.support.TransactionTemplate(transactionManager);
        concurrent.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        doAnswer(call -> {
            var result = selectOrder(id);
            int read = reads.incrementAndGet();
            if (read <= 3) {
                concurrent.executeWithoutResult(status -> jdbc.update(
                        "UPDATE orders SET version = version + 1, status = ? WHERE id = ?",
                        terminalWinner && read == 3 ? "CONFIRMED" : "PENDING_INVENTORY", id));
            }
            return result;
        }).when(orders).findByIdWithLines(id);
        if (terminalWinner) {
            assertThat(progress.apply(id, rejected()).status()).isEqualTo(OrderStatus.CONFIRMED);
        } else {
            assertThatThrownBy(() -> progress.apply(id, rejected()))
                    .isInstanceOf(org.springframework.dao.OptimisticLockingFailureException.class);
        }
        assertThat(reads.get()).isEqualTo(4);
        assertThat(progress.load(id).order().rejectionReason()).isNull();
    }

    @Test
    void concurrentFinalizersReloadWinnerInFreshTransactionAfterOptimisticRollback() throws Exception {
        UUID id = pending();
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicInteger reads = new AtomicInteger();
        List<Long> transactions = new CopyOnWriteArrayList<>();
        doAnswer(call -> {
            var result = selectOrder(id);
            transactions.add(jdbc.queryForObject("SELECT txid_current()", Long.class));
            if (reads.incrementAndGet() <= 2) barrier.await(10, TimeUnit.SECONDS);
            return result;
        }).when(orders).findByIdWithLines(id);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> progress.apply(id, reserved(id)));
            var b = executor.submit(() -> progress.apply(id, rejected()));
            assertThat(a.get(20, TimeUnit.SECONDS)).isEqualTo(b.get(20, TimeUnit.SECONDS));
        }
        assertThat(transactions).hasSize(3).doesNotHaveDuplicates();
        assertThat(jdbc.queryForObject("SELECT version FROM orders WHERE id = ?", Long.class, id)).isEqualTo(1L);
    }
}
