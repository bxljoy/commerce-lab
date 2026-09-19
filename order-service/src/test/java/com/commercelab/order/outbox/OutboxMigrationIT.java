package com.commercelab.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.commercelab.order.AbstractPostgresIntegrationTest;
import com.commercelab.order.domain.Money;
import com.commercelab.order.domain.Order;
import com.commercelab.order.domain.OrderLine;
import com.commercelab.order.repository.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Currency;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest
class OutboxMigrationIT extends AbstractPostgresIntegrationTest {
    @Autowired DataSource dataSource;
    @Autowired JdbcTemplate jdbc;
    @Autowired OrderOutboxStore store;
    @Autowired OrderPlacedEventFactory factory;
    @Autowired OrderRepository orders;
    @Autowired ObjectMapper mapper;
    @Autowired PlatformTransactionManager transactionManager;
    private final List<UUID> ownedOrders = new ArrayList<>();

    @AfterEach
    void removeOwnedOrders() {
        for (UUID id : ownedOrders) {
            jdbc.update("DELETE FROM order_outbox WHERE order_id = ?", id);
            jdbc.update("DELETE FROM order_lines WHERE order_id = ?", id);
            jdbc.update("DELETE FROM orders WHERE id = ?", id);
        }
    }

    @Test
    void freshSchemaHasEmptyOutboxAndVersionFive() throws Exception {
        inSchema((schema, db) -> {
            migrate(schema, "5");
            assertThat(db.queryForObject("SELECT count(*) FROM order_outbox", Long.class)).isZero();
            assertThat(db.queryForList("SELECT version FROM flyway_schema_history WHERE version IS NOT NULL "
                    + "ORDER BY installed_rank", String.class)).containsExactly("1", "2", "3", "4", "5");
            assertThat(db.queryForObject("SELECT indexdef FROM pg_indexes WHERE schemaname = ? "
                    + "AND indexname = 'ix_order_outbox_due'", String.class, schema))
                    .contains("next_attempt_at, created_at, event_id", "WHERE (delivered_at IS NULL)");
        });
    }

    @Test
    void upgradeLeavesTerminalOrdersLinesAndRequestIdentitiesUnchanged() throws Exception {
        inSchema((schema, db) -> {
            migrate(schema, "4");
            for (String status : List.of("PLACED", "CONFIRMED", "REJECTED")) seed(db, status, false);
            var before = snapshot(db);
            migrate(schema, "5");
            assertThat(snapshot(db)).isEqualTo(before);
            assertThat(db.queryForObject("SELECT count(*) FROM order_outbox", Long.class)).isZero();
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void pendingOrdersAbortUpgradeWithoutChangingRowsOrVersionFourHistory(boolean blocked) throws Exception {
        inSchema((schema, db) -> {
            migrate(schema, "4");
            seed(db, "PENDING_INVENTORY", blocked);
            seed(db, "CONFIRMED", false);
            var before = snapshot(db);
            var history = db.queryForList("SELECT * FROM flyway_schema_history ORDER BY installed_rank");
            assertThatThrownBy(() -> migrate(schema, "5"))
                    .hasStackTraceContaining("Phase 4A requires resolving all Phase 3B pending orders before migration");
            assertThat(snapshot(db)).isEqualTo(before);
            assertThat(db.queryForList("SELECT * FROM flyway_schema_history ORDER BY installed_rank"))
                    .isEqualTo(history);
            assertThat(db.queryForObject("SELECT count(*) FROM information_schema.tables "
                    + "WHERE table_schema = ? AND table_name = 'order_outbox'", Long.class, schema)).isZero();
        });
    }

    @Test
    void insertFlushesJpaAndPreservesExactTextAndDeliveryDefaults() {
        Order order = order();
        OutboxMessage original = factory.create(order, "proof");
        OutboxMessage message = new OutboxMessage(original.eventId(), original.orderId(), original.topic(),
                original.messageKey(), " \n" + original.payload() + "\n ");
        transaction().executeWithoutResult(tx -> {
            orders.add(order);
            store.insert(message);
        });
        var row = jdbc.queryForMap("SELECT * FROM order_outbox WHERE event_id = ?", message.eventId());
        assertThat(row.get("payload")).isEqualTo(message.payload());
        assertThat(row.get("attempt_count")).isEqualTo(0L);
        assertThat(row.get("event_type")).isEqualTo("OrderPlaced");
        assertThat(row.get("schema_version")).isEqualTo(1);
        assertThat(row.get("created_at")).isNotNull();
        assertThat(row.get("next_attempt_at")).isNotNull();
        for (String field : List.of("lease_token", "lease_until", "last_error_code", "delivered_at")) {
            assertThat(row.get(field)).isNull();
        }
        assertThatThrownBy(() -> transaction().executeWithoutResult(tx -> store.insert(message)))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThatThrownBy(() -> transaction().executeWithoutResult(tx ->
                store.insert(factory.create(order, "another"))))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    }

    @Test
    void insertRequiresCallerTransactionAndRollsBackWithOrder() {
        Order order = order();
        OutboxMessage message = factory.create(order, "proof");
        assertThatThrownBy(() -> store.insert(message)).isInstanceOf(InvalidDataAccessApiUsageException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
        transaction().executeWithoutResult(tx -> {
            orders.add(order);
            store.insert(message);
            tx.setRollbackOnly();
        });
        assertThat(jdbc.queryForObject("SELECT count(*) FROM orders WHERE id = ?", Long.class, order.id())).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_outbox WHERE event_id = ?", Long.class,
                message.eventId())).isZero();
    }

    @Test
    void insertExplicitlyRejectsInvalidEnvelopeBeforeDatabaseWrite() throws Exception {
        Order order = order();
        OutboxMessage valid = factory.create(order, "proof");
        for (String field : List.of("eventId", "orderId", "eventType", "schemaVersion")) {
            ObjectNode body = (ObjectNode) mapper.readTree(valid.payload());
            body.remove(field);
            assertRejected(valid, body.toString());
            body = (ObjectNode) mapper.readTree(valid.payload());
            body.put(field, "wrong");
            assertRejected(valid, body.toString());
        }
        for (String body : List.of("null", "[]", "{}", "not-json", "{\"schemaVersion\":1}",
                valid.payload() + " {}")) assertRejected(valid, body);
        for (OutboxMessage invalid : List.of(
                new OutboxMessage(valid.eventId(), valid.orderId(), "wrong", valid.messageKey(), valid.payload()),
                new OutboxMessage(valid.eventId(), valid.orderId(), valid.topic(), "wrong", valid.payload()))) {
            assertThatThrownBy(() -> transaction().executeWithoutResult(tx -> store.insert(invalid)))
                    .isInstanceOf(InvalidDataAccessApiUsageException.class)
                    .hasCauseInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void databaseIndependentlyEnforcesEnvelopeAndDeliveryConstraints() throws Exception {
        inSchema((schema, db) -> {
            migrate(schema, "5");
            UUID orderId = seed(db, "CONFIRMED", false);
            UUID eventId = UUID.randomUUID();
            String valid = "{\"eventId\":\"" + eventId + "\",\"orderId\":\"" + orderId
                    + "\",\"eventType\":\"OrderPlaced\",\"schemaVersion\":1}";
            for (String payload : List.of("{}", "null", "[]", "not-json", valid.replace("OrderPlaced", "Other"),
                    valid.replace(eventId.toString(), UUID.randomUUID().toString()),
                    valid.replace(orderId.toString(), UUID.randomUUID().toString()),
                    valid.replace(":1}", ":\"1\"}"), valid.replace(":1}", ":2}"))) {
                assertThatThrownBy(() -> rawInsert(db, eventId, orderId, payload))
                        .isInstanceOf(org.springframework.dao.DataAccessException.class);
            }
            rawInsert(db, eventId, orderId, valid);
            for (String assignment : List.of("attempt_count = -1", "lease_token = gen_random_uuid()",
                    "lease_until = CURRENT_TIMESTAMP", "event_type = 'Other'", "schema_version = 2",
                    "topic = 'wrong'", "message_key = 'wrong'")) {
                assertThatThrownBy(() -> db.update("UPDATE order_outbox SET " + assignment))
                        .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
            }
        });
    }

    private void assertRejected(OutboxMessage valid, String payload) {
        var invalid = new OutboxMessage(valid.eventId(), valid.orderId(), valid.topic(), valid.messageKey(), payload);
        assertThatThrownBy(() -> transaction().executeWithoutResult(tx -> store.insert(invalid)))
                .isInstanceOf(InvalidDataAccessApiUsageException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    private void rawInsert(JdbcTemplate db, UUID eventId, UUID orderId, String payload) {
        db.update("INSERT INTO order_outbox (event_id, order_id, event_type, schema_version, topic, message_key, payload) "
                + "VALUES (?, ?, 'OrderPlaced', 1, 'commerce.orders.v1', ?, ?)",
                eventId, orderId, orderId.toString(), payload);
    }

    private UUID seed(JdbcTemplate db, String status, boolean blocked) {
        UUID id = UUID.randomUUID();
        db.update("INSERT INTO orders (id, customer_id, status, currency, placed_at, recovery_blocked) "
                + "VALUES (?, 'historical', ?, 'EUR', '2026-01-01T00:00:00Z', ?)", id, status, blocked);
        db.update("INSERT INTO order_lines (id, order_id, sku, quantity, unit_price, line_position) "
                + "VALUES (?, ?, 'SKU', 2, 10, 0)", UUID.randomUUID(), id);
        db.update("INSERT INTO order_requests (request_key, order_id, canonical_payload, fingerprint, "
                + "fingerprint_version, correlation_id) VALUES (?, ?, '{}', 'fingerprint', 1, 'historical')",
                id.toString(), id);
        return id;
    }

    private Object snapshot(JdbcTemplate db) {
        return List.of(db.queryForList("SELECT * FROM orders ORDER BY id"),
                db.queryForList("SELECT * FROM order_lines ORDER BY id"),
                db.queryForList("SELECT * FROM order_requests ORDER BY request_key"));
    }

    private void migrate(String schema, String target) {
        Flyway.configure().dataSource(dataSource).schemas(schema).defaultSchema(schema)
                .locations("classpath:db/migration").target(target).load().migrate();
    }

    private void inSchema(SchemaCheck check) throws Exception {
        String schema = "outbox_upgrade_" + UUID.randomUUID().toString().replace("-", "");
        jdbc.execute("CREATE SCHEMA " + schema);
        try (var connection = dataSource.getConnection()) {
            String original = connection.getSchema();
            try {
                connection.setSchema(schema);
                check.run(schema, new JdbcTemplate(new SingleConnectionDataSource(connection, true)));
            } finally {
                connection.setSchema(original);
            }
        } finally {
            jdbc.execute("DROP SCHEMA " + schema + " CASCADE");
        }
    }

    private TransactionTemplate transaction() {
        return new TransactionTemplate(transactionManager);
    }

    private Order order() {
        Order order = Order.place("outbox-test", List.of(new OrderLine("SKU", 2,
                new Money(BigDecimal.TEN, Currency.getInstance("EUR")))));
        ownedOrders.add(order.id());
        return order;
    }

    @FunctionalInterface
    interface SchemaCheck {
        void run(String schema, JdbcTemplate db) throws Exception;
    }
}
