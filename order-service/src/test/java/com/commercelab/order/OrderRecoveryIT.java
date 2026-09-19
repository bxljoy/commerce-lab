package com.commercelab.order;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.commercelab.order.domain.OrderStatus;
import com.commercelab.order.inventory.*;
import com.commercelab.order.service.*;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;
import org.slf4j.MDC;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.*;

class OrderRecoveryIT extends AbstractPostgresIntegrationTest {
    static final Instant START = Instant.parse("2026-09-19T12:00:00Z");
    static final AtomicReference<Instant> NOW = new AtomicReference<>(START);
    static final InventoryGateway GATEWAY = mock(InventoryGateway.class);
    ConfigurableApplicationContext context;
    JdbcTemplate jdbc;
    OrderCreationService creation;
    OrderProgressService progress;
    OrderReservationCoordinator coordinator;
    OrderRecoveryWorker worker;

    @TestConfiguration(proxyBeanMethods = false)
    static class Fixtures {
        @Bean @Primary InventoryGateway fixtureGateway() { return GATEWAY; }
        @Bean @Primary Clock fixtureClock() {
            return new Clock() {
                public ZoneId getZone() { return ZoneOffset.UTC; }
                public Clock withZone(ZoneId zone) { return this; }
                public Instant instant() { return NOW.get(); }
            };
        }
    }

    void open() {
        open(false);
    }

    void open(boolean scheduled) {
        context = new SpringApplicationBuilder(OrderServiceApplication.class, Fixtures.class)
                .run("--server.port=0", "--order.recovery.enabled=" + scheduled,
                        "--order.recovery.fixed-delay-ms=50",
                        "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword());
        jdbc = context.getBean(JdbcTemplate.class);
        creation = context.getBean(OrderCreationService.class);
        progress = context.getBean(OrderProgressService.class);
        coordinator = context.getBean(OrderReservationCoordinator.class);
        worker = new OrderRecoveryWorker(progress, coordinator, context.getBean(Clock.class), 20);
    }

    @BeforeEach void setup() {
        NOW.set(START);
        reset(GATEWAY);
        open();
        jdbc.execute("TRUNCATE order_requests, order_lines, orders CASCADE");
        when(GATEWAY.find(any(), any())).thenAnswer(call -> {
            outsideTransaction();
            return reserved(call.getArgument(0));
        });
        when(GATEWAY.reserve(any(), any())).thenAnswer(call -> {
            outsideTransaction();
            return reserved(((InventoryRequest) call.getArgument(0)).orderId());
        });
    }

    @AfterEach void close() { if (context != null) context.close(); MDC.clear(); }

    UUID pending() {
        return creation.createOrReplay(UUID.randomUUID().toString(), new PlaceOrderCommand("customer", "EUR",
                List.of(new PlaceOrderCommand.Line("A", 2, BigDecimal.ONE))), "origin:recovery").order().id();
    }

    static InventoryOutcome.Reserved reserved(UUID id) {
        return new InventoryOutcome.Reserved(id, List.of(new InventoryLine("A", 2)));
    }

    static void outsideTransaction() {
        assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
        assertThat(MDC.get("correlationId")).isEqualTo("origin:recovery");
    }

    @Test void reservedAndStoredRejectionFinalizeWithoutPost() {
        UUID accepted = pending(), rejected = pending();
        doReturn(new InventoryOutcome.Rejected(Map.of("A", new StockShortage(2, 0))))
                .when(GATEWAY).find(eq(rejected), any());
        NOW.set(START.plusSeconds(5));
        worker.runOnce();
        assertThat(progress.load(accepted).order().status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(progress.load(rejected).order().rejectionReason()).isEqualTo("STOCK_UNAVAILABLE");
        verify(GATEWAY, never()).reserve(any(), any());
        assertThat(MDC.getCopyOfContextMap()).isNullOrEmpty();
    }

    @Test void onlyTypedMissingPermitsOnePostAndPostFailureDoesNotRetry() {
        UUID id = pending();
        doReturn(new InventoryOutcome.Missing()).when(GATEWAY).find(eq(id), any());
        doThrow(new TransientInventoryException("INVENTORY_UNAVAILABLE")).when(GATEWAY).reserve(any(), any());
        coordinator.reconcile(id);
        verify(GATEWAY).find(id, "origin:recovery");
        verify(GATEWAY).reserve(new InventoryRequest(id, reserved(id).lines()), "origin:recovery");
        assertThat(progress.load(id).attemptCount()).isEqualTo(1);
        assertThat(progress.load(id).order().status()).isEqualTo(OrderStatus.PENDING_INVENTORY);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {
            "{\"B\":{\"requested\":2,\"available\":0}}",
            "{\"A\":{\"requested\":3,\"available\":0}}",
            "{}", "{\"A\":{\"requested\":2,\"available\":2}}",
            "{\"A\":{\"requested\":\"2\",\"available\":0}}"})
    void invalidRejectedGetStaysPendingAndBlockedWithoutPost(String shortages) {
        UUID id = pending();
        var builder = org.springframework.web.client.RestClient.builder();
        var server = org.springframework.test.web.client.MockRestServiceServer.bindTo(builder).build();
        var gateway = new RestInventoryGateway(builder.build(), new com.fasterxml.jackson.databind.ObjectMapper(),
                new InventoryClientConfiguration().inventoryCircuitBreaker());
        String body = "{\"type\":\"https://commerce-lab/errors/stock-unavailable\",\"unavailableSkus\":" + shortages + "}";
        server.expect(org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo("/api/v1/reservations"))
                .andExpect(org.springframework.test.web.client.match.MockRestRequestMatchers.method(org.springframework.http.HttpMethod.POST))
                .andRespond(org.springframework.test.web.client.response.MockRestResponseCreators
                        .withStatus(org.springframework.http.HttpStatus.CONFLICT)
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON).body(body));
        server.expect(org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo("/api/v1/reservations/" + id))
                .andExpect(org.springframework.test.web.client.match.MockRestRequestMatchers.method(org.springframework.http.HttpMethod.GET))
                .andRespond(org.springframework.test.web.client.response.MockRestResponseCreators
                        .withStatus(org.springframework.http.HttpStatus.CONFLICT)
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON).body(body));
        assertThatThrownBy(() -> gateway.reserve(new InventoryRequest(id, reserved(id).lines()), "origin:recovery"))
                .isInstanceOf(InventoryProtocolException.class);
        var recovery = new OrderReservationCoordinator(progress, gateway, context.getBean(Clock.class));
        recovery.reconcile(id);
        assertThat(progress.load(id).order().status()).isEqualTo(OrderStatus.PENDING_INVENTORY);
        assertThat(progress.load(id).order().rejectionReason()).isNull();
        assertThat(progress.load(id).order().recoveryIssue()).isEqualTo("INVENTORY_INVALID_RESPONSE");
        assertThat(jdbc.queryForObject("SELECT recovery_blocked FROM orders WHERE id=?", Boolean.class, id)).isTrue();
        recovery.reconcile(id);
        server.verify();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"A", "UNKNOWN"})
    void requestedShortageSubsetRemainsDefinitive(String sku) {
        UUID id = creation.createOrReplay(UUID.randomUUID().toString(), new PlaceOrderCommand("customer", "EUR",
                List.of(new PlaceOrderCommand.Line(sku, 2, BigDecimal.ONE),
                        new PlaceOrderCommand.Line("B", 1, BigDecimal.ONE))), "origin:recovery").order().id();
        doReturn(new InventoryOutcome.Rejected(Map.of(sku, new StockShortage(2, 0))))
                .when(GATEWAY).find(eq(id), any());
        coordinator.reconcile(id);
        assertThat(progress.load(id).order().status()).isEqualTo(OrderStatus.REJECTED);
        assertThat(progress.load(id).order().rejectionReason()).isEqualTo("STOCK_UNAVAILABLE");
        assertThat(progress.load(id).order().recoveryIssue()).isNull();
        assertThat(jdbc.queryForObject("SELECT recovery_blocked FROM orders WHERE id=?", Boolean.class, id)).isFalse();
        verify(GATEWAY, never()).reserve(any(), any());
    }

    @Test void protocolReleasedAndMismatchesBlockAndFreshLoadSkipsBlockedTerminalAndLegacy() {
        var outcomes = List.of(new InventoryOutcome.Released(UUID.randomUUID(), reserved(UUID.randomUUID()).lines()),
                reserved(UUID.randomUUID()), new InventoryOutcome.Reserved(UUID.randomUUID(), List.of(new InventoryLine("B", 2))));
        for (var outcome : outcomes) {
            UUID id = pending();
            doReturn(outcome).when(GATEWAY).find(eq(id), any());
            coordinator.reconcile(id);
            assertThat(progress.load(id).order().recoveryIssue()).isNotNull();
            coordinator.reconcile(id);
            verify(GATEWAY).find(id, "origin:recovery");
        }
        UUID lines = pending();
        doReturn(new InventoryOutcome.Reserved(lines, List.of(new InventoryLine("B", 2))))
                .when(GATEWAY).find(eq(lines), any());
        coordinator.reconcile(lines);
        assertThat(progress.load(lines).order().recoveryIssue()).isNotNull();
        UUID protocol = pending();
        doThrow(new InventoryProtocolException("INVENTORY_UNEXPECTED_4XX")).when(GATEWAY).find(eq(protocol), any());
        coordinator.reconcile(protocol);
        assertThat(progress.load(protocol).order().recoveryIssue()).isEqualTo("INVENTORY_UNEXPECTED_4XX");
        UUID terminal = pending(), legacy = pending();
        progress.apply(terminal, reserved(terminal));
        jdbc.update("UPDATE orders SET status='PLACED' WHERE id=?", legacy);
        coordinator.reconcile(terminal);
        coordinator.reconcile(legacy);
        NOW.set(START.plusSeconds(100));
        worker.runOnce();
        verify(GATEWAY, never()).find(eq(terminal), any());
        verify(GATEWAY, never()).find(eq(legacy), any());
        verify(GATEWAY, never()).reserve(any(), any());
        assertThat(progress.findDueIds(NOW.get(), 20)).isEmpty();
    }

    @Test void persistedExponentialDelayUsesFailedPassCountAndCapsWithNonnegativeJitter() {
        UUID id = pending();
        doThrow(new TransientInventoryException("INVENTORY_CIRCUIT_OPEN")).when(GATEWAY).find(eq(id), any());
        long[] delays = {5, 10, 20, 40, 60, 60, 60};
        for (int i = 0; i < delays.length; i++) {
            Instant before = NOW.get();
            coordinator.reconcile(id);
            Instant due = jdbc.queryForObject("SELECT next_attempt_at FROM orders WHERE id=?",
                    (rs, n) -> rs.getTimestamp(1).toInstant(), id);
            assertThat(due).isBetween(before.plusSeconds(delays[i]), before.plusSeconds(delays[i]).plusMillis(250));
            assertThat(progress.load(id).attemptCount()).isEqualTo(i + 1);
            assertThat(progress.findDueIds(due.minusMillis(1), 20)).isEmpty();
            assertThat(progress.findDueIds(due, 20)).containsExactly(id);
            NOW.set(due);
        }
        verify(GATEWAY, times(delays.length)).find(eq(id), any());
        verify(GATEWAY, never()).reserve(any(), any());
    }

    @Test void dueBoundaryOrderingAndBatchLimit() {
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 23; i++) ids.add(pending());
        worker.runOnce();
        verify(GATEWAY, never()).find(any(), any());
        NOW.set(START.plusSeconds(5));
        List<UUID> due = progress.findDueIds(NOW.get(), 20);
        assertThat(due).hasSize(20);
        assertThat(due.stream().map(UUID::toString).toList()).isSorted();
        worker.runOnce();
        verify(GATEWAY, times(20)).find(any(), any());
        assertThat(progress.findDueIds(NOW.get(), 20)).hasSize(3);
        worker.runOnce();
        verify(GATEWAY, times(23)).find(any(), any());
    }

    @Test void pendingAndDeferredRowsSurviveActualContextRestart() {
        UUID pending = pending(), deferred = pending();
        doThrow(new TransientInventoryException("INVENTORY_UNAVAILABLE")).when(GATEWAY).find(eq(deferred), any());
        coordinator.reconcile(deferred);
        var persisted = jdbc.queryForMap("SELECT attempt_count, next_attempt_at, last_failure_code FROM orders WHERE id=?", deferred);
        context.close();
        assertThat(context.isActive()).isFalse();
        open();
        assertThat(jdbc.queryForMap("SELECT attempt_count, next_attempt_at, last_failure_code FROM orders WHERE id=?", deferred))
                .isEqualTo(persisted);
        doReturn(new InventoryOutcome.Missing()).when(GATEWAY).find(eq(pending), any());
        doReturn(reserved(deferred)).when(GATEWAY).find(eq(deferred), any());
        NOW.set(START.plusSeconds(6));
        worker.runOnce();
        assertThat(progress.load(pending).order().status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(progress.load(deferred).order().status()).isEqualTo(OrderStatus.CONFIRMED);
        verify(GATEWAY, times(1)).reserve(any(), any());
    }

    @Test void remoteCommitThenLostLocalRecordingRecoversAfterActualContextRestart() {
        UUID id = pending();
        var remote = new ConcurrentHashMap<UUID, InventoryOutcome>();
        doAnswer(call -> {
            outsideTransaction();
            remote.put(id, reserved(id));
            throw new IllegalStateException("simulated process loss after remote commit before local apply");
        }).when(GATEWAY).reserve(any(), any());
        assertThatThrownBy(() -> coordinator.attempt(id)).isInstanceOf(IllegalStateException.class);
        assertThat(progress.load(id).order().status()).isEqualTo(OrderStatus.PENDING_INVENTORY);
        context.close();
        assertThat(context.isActive()).isFalse();
        doAnswer(call -> { outsideTransaction(); return remote.get(id); }).when(GATEWAY).find(eq(id), any());
        NOW.set(START.plusSeconds(5));
        open(true);
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(progress.load(id).order().status()).isEqualTo(OrderStatus.CONFIRMED));
        assertThat(progress.load(id).order().status()).isEqualTo(OrderStatus.CONFIRMED);
        verify(GATEWAY, times(1)).reserve(any(), any());
        assertThat(remote).hasSize(1);
    }

    @Test void twoWorkersAndRequestConvergeWithoutStaleDeferredOrBlockedOverwrite() throws Exception {
        UUID id = pending();
        NOW.set(START.plusSeconds(5));
        var entered = new CountDownLatch(3);
        var release = new CountDownLatch(1);
        var remote = new ConcurrentHashMap<UUID, InventoryRequest>();
        doAnswer(call -> {
            outsideTransaction(); entered.countDown();
            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
            return new InventoryOutcome.Missing();
        }).when(GATEWAY).find(eq(id), any());
        doAnswer(call -> {
            outsideTransaction(); entered.countDown();
            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
            InventoryRequest request = call.getArgument(0);
            remote.compute(request.orderId(), (key, previous) -> {
                if (previous != null) assertThat(request).isEqualTo(previous);
                return request;
            });
            return reserved(id);
        }).when(GATEWAY).reserve(any(), any());
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = pool.submit(worker::runOnce);
            var second = pool.submit(worker::runOnce);
            var request = pool.submit(() -> coordinator.attempt(id));
            try { assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue(); }
            finally { release.countDown(); }
            first.get(10, TimeUnit.SECONDS); second.get(10, TimeUnit.SECONDS); request.get(10, TimeUnit.SECONDS);
        }
        progress.defer(id, "INVENTORY_UNAVAILABLE", NOW.get().plusSeconds(60));
        progress.block(id, "INVENTORY_RELEASED");
        assertThat(progress.load(id).order().status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(progress.load(id).order().recoveryIssue()).isNull();
        assertThat(jdbc.queryForObject("SELECT version FROM orders WHERE id=?", Long.class, id)).isEqualTo(1);
        assertThat(remote).hasSize(1);
        verify(GATEWAY, times(3)).reserve(any(), any());
    }

    @Test void concurrentTransientPassesAdvanceBackoffFromCurrentPersistedCount() throws Exception {
        UUID id = pending();
        NOW.set(START.plusSeconds(5));
        var bothLoaded = new CyclicBarrier(2);
        doAnswer(call -> {
            outsideTransaction();
            bothLoaded.await(10, TimeUnit.SECONDS);
            throw new TransientInventoryException("INVENTORY_UNAVAILABLE");
        }).when(GATEWAY).find(eq(id), any());
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = pool.submit(worker::runOnce);
            var second = pool.submit(worker::runOnce);
            first.get(15, TimeUnit.SECONDS);
            second.get(15, TimeUnit.SECONDS);
        }
        assertThat(progress.load(id).attemptCount()).isEqualTo(2);
        Instant due = jdbc.queryForObject("SELECT next_attempt_at FROM orders WHERE id=?",
                (rs, n) -> rs.getTimestamp(1).toInstant(), id);
        assertThat(due).isBetween(NOW.get().plusSeconds(10), NOW.get().plusMillis(10250));
        verify(GATEWAY, times(2)).find(eq(id), any());
        verify(GATEWAY, never()).reserve(any(), any());
    }

    @Test void callerTransactionIsRejectedBeforeAnyGatewayEntry() {
        UUID id = pending();
        var tx = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
        assertThatThrownBy(() -> tx.executeWithoutResult(status -> coordinator.reconcile(id)))
                .isInstanceOf(org.springframework.transaction.IllegalTransactionStateException.class);
        verify(GATEWAY, never()).find(any(), any());
    }

    @Test void requestDeferralCountsTowardRecoveryBackoff() {
        UUID id = pending();
        doThrow(new TransientInventoryException("INVENTORY_CIRCUIT_OPEN")).when(GATEWAY).reserve(any(), any());
        coordinator.attempt(id);
        assertThat(progress.load(id).attemptCount()).isEqualTo(1);
        doThrow(new TransientInventoryException("INVENTORY_UNAVAILABLE")).when(GATEWAY).find(eq(id), any());
        coordinator.reconcile(id);
        Instant due = jdbc.queryForObject("SELECT next_attempt_at FROM orders WHERE id=?",
                (rs, n) -> rs.getTimestamp(1).toInstant(), id);
        assertThat(due).isBetween(START.plusSeconds(10), START.plusMillis(10250));
        assertThat(progress.load(id).attemptCount()).isEqualTo(2);
        verify(GATEWAY, times(1)).reserve(any(), any());
        verify(GATEWAY, times(1)).find(any(), any());
    }

    @Test void missingGetNeverMakesInvalidPostOutcomeDefinitive() {
        for (String scenario : List.of("missing", "released", "wrong-id", "wrong-lines", "null")) {
            UUID id = pending();
            InventoryOutcome outcome = switch (scenario) {
                case "missing" -> new InventoryOutcome.Missing();
                case "released" -> new InventoryOutcome.Released(id, reserved(id).lines());
                case "wrong-id" -> reserved(UUID.randomUUID());
                case "wrong-lines" -> new InventoryOutcome.Reserved(id, List.of(new InventoryLine("B", 2)));
                default -> null;
            };
            doReturn(new InventoryOutcome.Missing()).when(GATEWAY).find(eq(id), any());
            doReturn(outcome).when(GATEWAY).reserve(eq(new InventoryRequest(id, reserved(id).lines())), any());
            coordinator.reconcile(id);
            assertThat(progress.load(id).order().status()).isEqualTo(OrderStatus.PENDING_INVENTORY);
            assertThat(progress.load(id).order().recoveryIssue()).isNotNull();
            verify(GATEWAY).find(eq(id), any());
            verify(GATEWAY).reserve(eq(new InventoryRequest(id, reserved(id).lines())), any());
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"transient", "protocol", "mismatched-rejection"})
    void lateRecoveryFailureCannotOverwriteRequestFinalization(String failure) throws Exception {
        UUID id = pending();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        doAnswer(call -> {
            outsideTransaction(); entered.countDown();
            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
            if (failure.equals("mismatched-rejection")) {
                return new InventoryOutcome.Rejected(Map.of("B", new StockShortage(2, 0)));
            }
            if (failure.equals("protocol")) throw new InventoryProtocolException("INVENTORY_RELEASED");
            throw new TransientInventoryException("INVENTORY_UNAVAILABLE");
        }).when(GATEWAY).find(eq(id), any());
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var recovery = pool.submit(() -> {
                try { coordinator.reconcile(id); }
                finally { assertThat(MDC.getCopyOfContextMap()).isNullOrEmpty(); }
            });
            try {
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                // A separate transaction and request path can progress while GET is blocked.
                var request = pool.submit(() -> coordinator.attempt(id));
                assertThat(request.get(5, TimeUnit.SECONDS).status()).isEqualTo(OrderStatus.CONFIRMED);
            } finally { release.countDown(); }
            recovery.get(10, TimeUnit.SECONDS);
        }
        assertThat(progress.load(id).attemptCount()).isZero();
        assertThat(progress.load(id).order().recoveryIssue()).isNull();
        assertThat(progress.load(id).order().status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(jdbc.queryForObject("SELECT version FROM orders WHERE id=?", Long.class, id)).isEqualTo(1);
    }
}
