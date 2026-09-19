package com.commercelab.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;

class FlywayMigrationIT extends AbstractPostgresIntegrationTest {

    @Test
    void versionFourSchedulesExistingPendingButNotHistoricalOrders() throws Exception {
        String schema = "phase3_upgrade_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
            migrate(schema, MigrationVersion.fromVersion("3"));
            statement.execute("INSERT INTO " + schema + ".orders (id, customer_id, status, currency, placed_at) VALUES "
                    + "(gen_random_uuid(), 'old', 'PLACED', 'EUR', '2026-01-01T00:00:00Z'),"
                    + "(gen_random_uuid(), 'pending', 'PENDING_INVENTORY', 'EUR', '2026-01-01T00:00:00Z')");
            migrate(schema, null);
            try (ResultSet rows = statement.executeQuery("SELECT status, version, attempt_count, recovery_blocked, "
                    + "next_attempt_at, placed_at FROM " + schema + ".orders ORDER BY customer_id")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("status")).isEqualTo("PLACED");
                assertThat(rows.getLong("version")).isZero();
                assertThat(rows.getInt("attempt_count")).isZero();
                assertThat(rows.getBoolean("recovery_blocked")).isFalse();
                assertThat(rows.getObject("next_attempt_at")).isNull();
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("status")).isEqualTo("PENDING_INVENTORY");
                assertThat(rows.getLong("version")).isZero();
                assertThat(rows.getObject("next_attempt_at", OffsetDateTime.class))
                        .isEqualTo(rows.getObject("placed_at", OffsetDateTime.class).plusSeconds(5));
            }
        } finally {
            try (Connection connection = connection(); Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
            }
        }
    }

    @Test
    void versionTwoBackfillsExistingLinesDeterministically() throws Exception {
        String schema = "phase2_upgrade_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = connection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA " + schema);
            }

            migrate(schema, MigrationVersion.fromVersion("1"));
            insertVersionOneOrder(connection, schema);
            migrate(schema, null);

            List<String> orderedSkus = new ArrayList<>();
            try (Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery(
                            "SELECT sku FROM " + schema
                                    + ".order_lines ORDER BY line_position")) {
                while (result.next()) {
                    orderedSkus.add(result.getString("sku"));
                }
            }

            assertThat(orderedSkus).containsExactly("SKU-FIRST", "SKU-SECOND");
            try (Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery("SELECT status FROM " + schema + ".orders")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString(1)).isEqualTo("PLACED");
            }
            try (Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery("SELECT count(*) FROM " + schema + ".order_requests")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getInt(1)).isZero();
            }
        } finally {
            try (Connection connection = connection(); Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
            }
        }
    }

    private static Connection connection() throws Exception {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static void migrate(String schema, MigrationVersion target) {
        var configuration = Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema)
                .defaultSchema(schema)
                .locations("classpath:db/migration");
        if (target != null) {
            configuration.target(target);
        }
        configuration.load().migrate();
    }

    private static void insertVersionOneOrder(Connection connection, String schema) throws Exception {
        UUID orderId = UUID.randomUUID();
        try (PreparedStatement order = connection.prepareStatement(
                "INSERT INTO " + schema
                        + ".orders (id, customer_id, status, currency, placed_at) VALUES (?, ?, ?, ?, ?)")) {
            order.setObject(1, orderId);
            order.setString(2, "migration-customer");
            order.setString(3, "PLACED");
            order.setString(4, "EUR");
            order.setObject(5, OffsetDateTime.now());
            order.executeUpdate();
        }

        // Insert in reverse UUID order. V2 documents UUID ordering as the deterministic
        // backfill policy because V1 did not preserve the original request sequence.
        insertLine(connection, schema, UUID.fromString("00000000-0000-0000-0000-000000000002"),
                orderId, "SKU-SECOND");
        insertLine(connection, schema, UUID.fromString("00000000-0000-0000-0000-000000000001"),
                orderId, "SKU-FIRST");
    }

    private static void insertLine(
            Connection connection, String schema, UUID id, UUID orderId, String sku) throws Exception {
        try (PreparedStatement line = connection.prepareStatement(
                "INSERT INTO " + schema
                        + ".order_lines (id, order_id, sku, quantity, unit_price) VALUES (?, ?, ?, ?, ?)")) {
            line.setObject(1, id);
            line.setObject(2, orderId);
            line.setString(3, sku);
            line.setInt(4, 1);
            line.setBigDecimal(5, java.math.BigDecimal.ONE);
            line.executeUpdate();
        }
    }
}
