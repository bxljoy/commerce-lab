package com.commercelab.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.UUID;
import java.sql.Timestamp;
import java.time.Instant;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;

@SpringBootTest
@AutoConfigureMockMvc
class InventoryMigrationIT {
    private static final PostgreSQLContainer<?> POSTGRES =
            AbstractPostgresIntegrationTest.POSTGRES;
    private static final UUID RESERVED = UUID.randomUUID();
    private static final UUID RELEASED = UUID.randomUUID();
    // UTF-16 Java ordering and PostgreSQL C/UTF-8 ordering disagree for these code points.
    private static final String BMP = "\uE000";
    private static final String SUPPLEMENTARY = "\uD800\uDC00";

    @DynamicPropertySource
    static void legacyDatabase(DynamicPropertyRegistry registry) {
        String base = POSTGRES.getJdbcUrl();
        String url = base + (base.contains("?") ? "&" : "?") + "currentSchema=upgrade_fixture";
        Flyway.configure().dataSource(url, POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas("upgrade_fixture").target("2").load().migrate();
        JdbcTemplate db = new JdbcTemplate(new DriverManagerDataSource(
                url, POSTGRES.getUsername(), POSTGRES.getPassword()));
        db.execute("ALTER TABLE inventory_reservation_lines ALTER COLUMN sku TYPE VARCHAR(64) COLLATE \"C\"");
        db.update("INSERT INTO stock VALUES (?, 8), (?, 8)", BMP, SUPPLEMENTARY);
        for (UUID id : new UUID[] {RESERVED, RELEASED}) {
            db.update("INSERT INTO inventory_reservations VALUES (?, ?, now(), ?)", id,
                    id.equals(RESERVED) ? "RESERVED" : "RELEASED",
                    id.equals(RESERVED) ? null : Timestamp.from(Instant.now()));
            db.update("INSERT INTO inventory_reservation_lines VALUES (?, ?, 1, 0), (?, ?, 1, 1)",
                    id, BMP, id, SUPPLEMENTARY);
        }
        registry.add("spring.datasource.url", () -> url);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired MockMvc mvc;
    @Autowired JdbcTemplate jdbc;

    @Test
    void backfillsBothStatesAndReplaysSemanticallyWithoutChangingLineOrderOrStock() throws Exception {
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_reservation_attempts WHERE outcome = 'RESERVED'", Integer.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT canonical_payload -> 0 ->> 'sku' FROM inventory_reservation_attempts WHERE order_id = ?",
                String.class, RESERVED)).isEqualTo(BMP);
        assertThat(SUPPLEMENTARY.compareTo(BMP)).isNegative();
        for (UUID id : new UUID[] {RESERVED, RELEASED}) {
            mvc.perform(post("/api/v1/reservations").contentType(MediaType.APPLICATION_JSON)
                    .characterEncoding("UTF-8")
                    .content("""
                            {"orderId":"%s","lines":[{"sku":"%s","quantity":1},{"sku":"%s","quantity":1}]}
                            """.formatted(id, SUPPLEMENTARY, BMP)))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value(id.equals(RESERVED) ? "RESERVED" : "RELEASED"))
                    .andExpect(jsonPath("$.lines[0].sku").value(BMP));
        }
        assertThat(jdbc.queryForList("SELECT available_quantity FROM stock WHERE sku IN (?, ?)", Integer.class, BMP, SUPPLEMENTARY))
                .containsExactly(8, 8);
    }
}
