package com.commercelab.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.commercelab.inventory.service.InventoryService;
import com.commercelab.inventory.service.ReserveInventoryCommand;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

@SpringBootTest
@AutoConfigureMockMvc
class InventoryReplayIT extends AbstractPostgresIntegrationTest {
    @Autowired MockMvc mvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired InventoryService service;

    @BeforeEach
    void reset() {
        jdbc.execute("TRUNCATE inventory_reservation_attempts, inventory_reservations, inventory_reservation_lines, stock CASCADE");
        jdbc.update("INSERT INTO stock VALUES ('A', 10), ('B', 10)");
    }

    @Test
    void reorderedReplayConflictsOnlyOnChangedPayloadAndReleaseIsTerminal() throws Exception {
        UUID id = UUID.randomUUID();
        reserve(id, lines()).andExpect(status().isCreated());
        reserve(id, reversed()).andExpect(status().isOk())
                .andExpect(jsonPath("$.lines[0].sku").value("B"));
        reserve(id, "[{\"sku\":\"A\",\"quantity\":3}]")
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.type").value("https://commerce-lab/errors/reservation-payload-conflict"));
        assertStock(8);
        mvc.perform(put("/api/v1/reservations/{id}/release", id)).andExpect(status().isOk());
        reserve(id, reversed()).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("RELEASED"));
        assertStock(10);
    }

    @Test
    void concurrentMatchingPayloadsHaveOneCreationAndOneStockEffect() throws Exception {
        UUID id = UUID.randomUUID();
        CyclicBarrier barrier = new CyclicBarrier(2);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                return reserve(id, lines()).andReturn().getResponse().getStatus();
            });
            var second = executor.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                return reserve(id, reversed()).andReturn().getResponse().getStatus();
            });
            assertThat(List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(201, 200);
        }
        assertStock(8);
    }

    @Test
    void rejectionCommitsSnapshotIncludingUnknownSkuAndReplaysAfterReplenishment() throws Exception {
        UUID id = UUID.randomUUID();
        String lines = "[{\"sku\":\"A\",\"quantity\":11},{\"sku\":\"UNKNOWN\",\"quantity\":1}]";
        reserve(id, lines).andExpect(status().isConflict());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_reservations", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_reservation_attempts WHERE order_id = ? AND outcome = 'REJECTED'", Integer.class, id)).isOne();
        assertStock(10);
        jdbc.update("UPDATE stock SET available_quantity = 100");
        jdbc.update("INSERT INTO stock VALUES ('UNKNOWN', 100)");
        reserve(id, lines).andExpect(status().isConflict())
                .andExpect(jsonPath("$.unavailableSkus.A.available").value(10))
                .andExpect(jsonPath("$.unavailableSkus.UNKNOWN.available").value(0));
        mvc.perform(get("/api/v1/reservations/{id}", id)).andExpect(status().isConflict())
                .andExpect(jsonPath("$.type").value("https://commerce-lab/errors/stock-unavailable"))
                .andExpect(jsonPath("$.unavailableSkus.A.requested").value(11))
                .andExpect(jsonPath("$.unavailableSkus.A.available").value(10))
                .andExpect(jsonPath("$.unavailableSkus.UNKNOWN.available").value(0));
        mvc.perform(put("/api/v1/reservations/{id}/release", id)).andExpect(status().isNotFound());
        reserve(id, reversed()).andExpect(status().isConflict())
                .andExpect(jsonPath("$.type").value("https://commerce-lab/errors/reservation-payload-conflict"));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_reservation_attempts WHERE order_id = ?",
                Integer.class, id)).isOne();
        assertThat(jdbc.queryForList("SELECT available_quantity FROM stock", Integer.class))
                .containsOnly(100);
    }

    @Test
    void failureCompletingLedgerRollsBackClaimReservationAndStockThenAllowsFreshRetry() throws Exception {
        UUID id = UUID.randomUUID();
        jdbc.execute("ALTER TABLE inventory_reservation_attempts ADD CONSTRAINT test_completion_failure CHECK (outcome <> 'RESERVED')");
        try {
            assertThatThrownBy(() -> service.reserve(new ReserveInventoryCommand(id,
                    List.of(new ReserveInventoryCommand.Line("A", 2), new ReserveInventoryCommand.Line("B", 2)))))
                    .isInstanceOf(DataIntegrityViolationException.class);
        } finally {
            jdbc.execute("ALTER TABLE inventory_reservation_attempts DROP CONSTRAINT test_completion_failure");
        }
        assertStock(10);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_reservations", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_reservation_attempts", Integer.class)).isZero();
        mvc.perform(get("/api/v1/reservations/{id}", id)).andExpect(status().isNotFound());
        reserve(id, lines()).andExpect(status().isCreated());
        assertStock(8);
    }

    @Test
    void invalidPayloadDoesNotConsumeIdentity() throws Exception {
        UUID id = UUID.randomUUID();
        reserve(id, "[{\"sku\":\"A\",\"quantity\":1},{\"sku\":\"A\",\"quantity\":1}]")
                .andExpect(status().isBadRequest());
        reserve(id, "[{\"sku\":\"A\",\"quantity\":0}]").andExpect(status().isBadRequest());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_reservation_attempts", Integer.class)).isZero();
        assertStock(10);
        reserve(id, lines()).andExpect(status().isCreated());
    }

    private ResultActions reserve(UUID id, String lines) throws Exception {
        return mvc.perform(post("/api/v1/reservations").contentType(MediaType.APPLICATION_JSON)
                .content("{\"orderId\":\"" + id + "\",\"lines\":" + lines + "}"));
    }

    private void assertStock(int quantity) {
        assertThat(jdbc.queryForList("SELECT available_quantity FROM stock ORDER BY sku", Integer.class))
                .containsExactly(quantity, quantity);
    }

    private static String lines() {
        return "[{\"sku\":\"B\",\"quantity\":2},{\"sku\":\"A\",\"quantity\":2}]";
    }

    private static String reversed() {
        return "[{\"sku\":\"A\",\"quantity\":2},{\"sku\":\"B\",\"quantity\":2}]";
    }
}
