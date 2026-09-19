package com.commercelab.order;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.commercelab.order.persistence.JpaOrderRepository;
import com.commercelab.order.persistence.OrderRequestStore;
import com.commercelab.order.service.OrderCreationService;
import com.commercelab.order.service.PlaceOrderCommand;
import com.commercelab.order.domain.IdempotencyConflictException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.List;
import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@ExtendWith(OutputCaptureExtension.class)
class OrderIdempotencyIT extends AbstractPostgresIntegrationTest {
    @Autowired OrderCreationService creation;
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @SpyBean JpaOrderRepository repository;
    @SpyBean OrderRequestStore requests;

    private static PlaceOrderCommand command(String price) {
        return new PlaceOrderCommand("cust", "EUR", List.of(
                new PlaceOrderCommand.Line("B", 2, new BigDecimal(price)),
                new PlaceOrderCommand.Line("A", 1, BigDecimal.TEN)));
    }

    @BeforeEach
    void clear() {
        jdbc.execute("TRUNCATE order_outbox, order_requests, order_lines, orders CASCADE");
    }

    @Test
    void createsPendingAndReplaysScaleEquivalentPayloadWithCurrentState() throws Exception {
        var first = creation.createOrReplay("Key:1", command("9.9900"), "correlation");
        var replay = creation.createOrReplay("Key:1", command("9.99"), "other");
        assertThat(first.created()).isTrue();
        assertThat(first.order().status().name()).isEqualTo("PENDING_INVENTORY");
        assertThat(replay.created()).isFalse();
        assertThat(replay.order().id()).isEqualTo(first.order().id());
        assertThat(jdbc.queryForObject("SELECT fingerprint_version FROM order_requests", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT fingerprint FROM order_requests", String.class)).matches("[0-9a-f]{64}");
        assertThat(jdbc.queryForObject("SELECT correlation_id FROM order_requests", String.class)).isEqualTo("correlation");
        jdbc.update("UPDATE orders SET status = 'CONFIRMED' WHERE id = ?", first.order().id());
        mvc.perform(post("/api/v1/orders").header("Idempotency-Key", "Key:1")
                        .contentType(MediaType.APPLICATION_JSON).content(body("9.99")))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("CONFIRMED"))
                .andExpect(header().string("Location", "/api/v1/orders/" + first.order().id()))
                .andExpect(header().doesNotExist("Retry-After"));
        assertCounts(1);
    }

    @Test
    void canonicalContentNotFingerprintControlsConflictAndKeysAreCaseSensitive() {
        creation.createOrReplay("Key", command("9.99"), "c");
        jdbc.update("UPDATE order_requests SET fingerprint = repeat('0', 64)");
        assertThat(creation.createOrReplay("Key", command("9.9900"), "c").created()).isFalse();
        jdbc.update("UPDATE order_requests SET fingerprint = ?",
                com.commercelab.order.service.OrderPayload.from(command("8")).fingerprint());
        assertThatThrownBy(() -> creation.createOrReplay("Key", command("8"), "c"))
                .isInstanceOf(IdempotencyConflictException.class);
        var reversed = new PlaceOrderCommand("cust", "EUR", command("9.99").lines().reversed());
        assertThatThrownBy(() -> creation.createOrReplay("Key", reversed, "c"))
                .isInstanceOf(IdempotencyConflictException.class);
        assertThat(creation.createOrReplay("key", command("9.99"), "c").created()).isTrue();
        assertCounts(2);
    }

    @Test
    void invalidPayloadForExistingKeyIsBadRequestNotConflict() throws Exception {
        creation.createOrReplay("existing", command("1"), "c");
        mvc.perform(post("/api/v1/orders").header("Idempotency-Key", "existing")
                        .contentType(MediaType.APPLICATION_JSON).content(body("0.00001")))
                .andExpect(status().isBadRequest());
        assertCounts(1);
    }

    @Test
    void requiredAndInvalidKeysAndInvalidLinesLeaveNoIdentity() throws Exception {
        mvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content(body("1")))
                .andExpect(status().isBadRequest());
        for (String key : List.of("", "has space", "x".repeat(129), "non-ascii-ä")) {
            mvc.perform(post("/api/v1/orders").header("Idempotency-Key", key)
                            .contentType(MediaType.APPLICATION_JSON).content(body("1")))
                    .andExpect(status().isBadRequest());
        }
        for (String lines : List.of("[null]", "[]",
                "[{\"sku\":\"A\",\"quantity\":1,\"unitPrice\":1},{\"sku\":\"A\",\"quantity\":2,\"unitPrice\":1}]",
                "[{\"sku\":\"A\",\"quantity\":0,\"unitPrice\":1}]",
                "[{\"sku\":\"A\",\"quantity\":1,\"unitPrice\":0.00001}]")) {
            mvc.perform(post("/api/v1/orders").header("Idempotency-Key", "valid")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"customerId\":\"cust\",\"currency\":\"EUR\",\"lines\":" + lines + "}"))
                    .andExpect(status().isBadRequest());
        }
        assertThatThrownBy(() -> creation.createOrReplay(null, command("1"), "c"))
                .isInstanceOf(IllegalArgumentException.class);
        assertCounts(0);
    }

    @Test
    void httpPendingReplayAndConflict() throws Exception {
        for (int i = 0; i < 2; i++) {
            mvc.perform(post("/api/v1/orders").header("Idempotency-Key", "http")
                            .contentType(MediaType.APPLICATION_JSON).content(body("1")))
                    .andExpect(status().isAccepted()).andExpect(header().string("Retry-After", "5"))
                    .andExpect(header().exists("Location"))
                    .andExpect(jsonPath("$.status").value("PENDING_INVENTORY"));
        }
        mvc.perform(post("/api/v1/orders").header("Idempotency-Key", "http")
                        .contentType(MediaType.APPLICATION_JSON).content(body("2")))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.type").value("https://commerce-lab/errors/idempotency-conflict"));
        assertCounts(1);
    }

    @Test
    void concurrentMatchingRequestsRollbackLoserBeforeReplay(CapturedOutput output) throws Exception {
        race(false);
        assertThat(output.getAll()).doesNotContain("Key (request_key)=(race)");
    }

    @Test
    void concurrentConflictingRequestsRollbackLoserBeforeConflict() throws Exception {
        race(true);
    }

    private void race(boolean conflict) throws Exception {
        var barrier = new CyclicBarrier(2);
        var inserts = new AtomicInteger();
        var lookupTransactions = new java.util.concurrent.CopyOnWriteArrayList<Long>();
        doAnswer(call -> {
            assertThat(org.springframework.transaction.support.TransactionSynchronizationManager
                    .isActualTransactionActive()).isTrue();
            lookupTransactions.add(jdbc.queryForObject("SELECT txid_current()", Long.class));
            return call.callRealMethod();
        }).when(requests).find("race");
        doAnswer(call -> {
            inserts.incrementAndGet();
            barrier.await(10, TimeUnit.SECONDS);
            return call.callRealMethod();
        }).when(repository).add(any());
        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> outcome("1"));
            var second = executor.submit(() -> outcome(conflict ? "2" : "1.0000"));
            var results = List.of(first.get(20, TimeUnit.SECONDS), second.get(20, TimeUnit.SECONDS));
            assertThat(inserts.get()).isEqualTo(2);
            assertThat(results).containsExactlyInAnyOrder("created", conflict ? "conflict" : "replayed");
        }
        assertCounts(1);
        assertThat(lookupTransactions).hasSize(3).doesNotHaveDuplicates();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_lines", Integer.class)).isEqualTo(2);
    }

    private String outcome(String price) {
        try {
            return creation.createOrReplay("race", command(price), "c").created() ? "created" : "replayed";
        } catch (IdempotencyConflictException ex) {
            return "conflict";
        }
    }

    @Test
    void unrelatedIntegrityFailureRollsBackOrderAndPropagates() {
        assertThatThrownBy(() -> creation.createOrReplay("valid", command("1"), "x".repeat(129)))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertCounts(0);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_lines", Integer.class)).isZero();
    }

    @Test
    void anotherUniqueConstraintIsNotTreatedAsAKeyRace() {
        creation.createOrReplay("first", command("1"), "c");
        jdbc.execute("CREATE UNIQUE INDEX test_request_fingerprint_unique ON order_requests(fingerprint)");
        try {
            assertThatThrownBy(() -> creation.createOrReplay("second", command("1"), "c"))
                    .isInstanceOf(DataIntegrityViolationException.class);
            assertCounts(1);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM order_lines", Integer.class)).isEqualTo(2);
        } finally {
            jdbc.execute("DROP INDEX test_request_fingerprint_unique");
        }
    }

    @Test
    void invalidCommandsNeverPersistEvenWhenCalledWithoutHttpValidation() {
        for (PlaceOrderCommand command : Arrays.asList(null,
                new PlaceOrderCommand("cust", "EUR", null),
                new PlaceOrderCommand("cust", "EUR", Arrays.asList((PlaceOrderCommand.Line) null)),
                new PlaceOrderCommand("cust", "EUR", List.of()),
                new PlaceOrderCommand("cust", "eur", command("1").lines()),
                new PlaceOrderCommand("cust", "ZZZ", command("1").lines()),
                new PlaceOrderCommand(" ", "EUR", command("1").lines()),
                new PlaceOrderCommand("cust", "EUR", List.of(
                        new PlaceOrderCommand.Line("A", 1, BigDecimal.ONE),
                        new PlaceOrderCommand.Line("A", 2, BigDecimal.ONE))),
                new PlaceOrderCommand("cust", "EUR", List.of(new PlaceOrderCommand.Line("A", 0, BigDecimal.ONE))),
                new PlaceOrderCommand("cust", "EUR", List.of(new PlaceOrderCommand.Line("A", 1, null))),
                command("0.00001"), command("1000000000000000"))) {
            assertThatThrownBy(() -> creation.createOrReplay("valid", command, "c"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertCounts(0);
        }
    }

    @Test
    void validKeyBoundariesAreAccepted() {
        assertThat(creation.createOrReplay("x", command("1"), "c").created()).isTrue();
        assertThat(creation.createOrReplay("A._:-" + "0".repeat(123), command("1"), "c").created()).isTrue();
        assertCounts(2);
    }

    private void assertCounts(int count) {
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_outbox", Integer.class)).isEqualTo(count);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM orders", Integer.class)).isEqualTo(count);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_requests", Integer.class)).isEqualTo(count);
    }

    private String body(String price) throws Exception {
        var command = command(price);
        var json = mapper.createObjectNode().put("customerId", command.customerId()).put("currency", "EUR");
        json.set("lines", mapper.valueToTree(command.lines()));
        return mapper.writeValueAsString(json);
    }
}
