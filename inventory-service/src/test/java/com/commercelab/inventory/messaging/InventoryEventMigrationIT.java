package com.commercelab.inventory.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.commercelab.inventory.AbstractPostgresIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

@SpringBootTest(properties = {"inventory.events.enabled=false", "spring.kafka.listener.auto-startup=false"})
class InventoryEventMigrationIT extends AbstractPostgresIntegrationTest {
    @Autowired DataSource dataSource;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper json;

    @Test
    void upgradePreservesReservedRejectedAndReleasedAttemptsWithoutBackfill() throws Exception {
        inSchema((schema, db) -> {
            migrate(schema, "3");
            for (String state : List.of("RESERVED", "REJECTED", "RELEASED")) seed(db, state);
            var before = snapshot(db);
            migrate(schema, "4");
            assertThat(snapshot(db)).isEqualTo(before);
            assertThat(db.queryForObject("SELECT count(*) FROM inventory_event_inbox", Long.class)).isZero();
            assertThat(db.queryForObject("SELECT count(*) FROM inventory_result_outbox", Long.class)).isZero();
            assertThat(db.queryForList("SELECT version FROM flyway_schema_history WHERE version IS NOT NULL "
                    + "ORDER BY installed_rank", String.class)).containsExactly("1", "2", "3", "4");
            assertThat(db.queryForObject("SELECT indexdef FROM pg_indexes WHERE schemaname = ? "
                    + "AND indexname = 'ix_inventory_result_outbox_due'", String.class, schema))
                    .contains("next_attempt_at, created_at, event_id", "WHERE (delivered_at IS NULL)");
        });
    }

    @Test
    void databaseRejectsMissingMismatchedIdentityAndInvalidDeliveryMetadata() throws Exception {
        inSchema((schema, db) -> {
            migrate(schema, "4");
            UUID order = seed(db, "REJECTED");
            UUID event = UUID.randomUUID();
            UUID cause = UUID.randomUUID();
            ObjectNode body = json.createObjectNode().put("eventId", event.toString())
                    .put("orderId", order.toString()).put("causationId", cause.toString())
                    .put("eventType", "InventoryRejected").put("schemaVersion", 1);
            for (String field : List.of("eventId", "orderId", "causationId", "eventType", "schemaVersion")) {
                ObjectNode missing = body.deepCopy();
                missing.remove(field);
                assertInvalid(db, event, order, cause, missing.toString());
                ObjectNode wrong = body.deepCopy();
                wrong.put(field, "wrong");
                assertInvalid(db, event, order, cause, wrong.toString());
                ObjectNode nil = body.deepCopy();
                nil.putNull(field);
                assertInvalid(db, event, order, cause, nil.toString());
            }
            for (String invalid : List.of("{}", "null", "[]", "not-json")) assertInvalid(db, event, order, cause, invalid);
            String payload = " \n" + body + "\n ";
            insert(db, event, order, cause, payload);
            var row = db.queryForMap("SELECT * FROM inventory_result_outbox");
            assertThat(row.get("payload")).isEqualTo(payload);
            assertThat(row.get("attempt_count")).isEqualTo(0L);
            assertThat(row.get("created_at")).isNotNull();
            assertThat(row.get("next_attempt_at")).isNotNull();
            for (String field : List.of("lease_token", "lease_until", "last_error_code", "delivered_at"))
                assertThat(row.get(field)).isNull();
            for (String assignment : List.of("attempt_count = -1", "lease_token = gen_random_uuid()",
                    "lease_until = CURRENT_TIMESTAMP", "event_type = 'Other'", "schema_version = 2",
                    "topic = 'wrong'", "message_key = 'wrong'")) {
                assertThatThrownBy(() -> db.update("UPDATE inventory_result_outbox SET " + assignment))
                        .isInstanceOf(DataAccessException.class);
            }
            UUID otherEvent = UUID.randomUUID();
            ObjectNode second = body.deepCopy().put("eventId", otherEvent.toString());
            assertInvalid(db, otherEvent, order, cause, second.toString());
            UUID otherOrder = seed(db, "REJECTED");
            second.put("orderId", otherOrder.toString());
            assertInvalid(db, otherEvent, otherOrder, cause, second.toString());
            UUID otherCause = UUID.randomUUID();
            second.put("orderId", order.toString()).put("causationId", otherCause.toString())
                    .put("eventType", "InventoryReserved");
            assertThatThrownBy(() -> db.update("""
                    INSERT INTO inventory_result_outbox
                    (event_id, order_id, causation_id, event_type, schema_version, topic, message_key, payload)
                    VALUES (?, ?, ?, 'InventoryReserved', 1, 'commerce.inventory.v1', ?, ?)
                    """, otherEvent, order, otherCause, order.toString(), second.toString()))
                    .isInstanceOf(DataAccessException.class);
        });
    }

    @Test
    void inboxIdentityIsScopedToConsumerAndRequiresProcessingMetadata() throws Exception {
        inSchema((schema, db) -> {
            migrate(schema, "4");
            UUID event = UUID.randomUUID();
            db.update("INSERT INTO inventory_event_inbox VALUES ('a', ?, '{\"schemaVersion\":1}', now())", event);
            db.update("INSERT INTO inventory_event_inbox VALUES ('b', ?, '{\"schemaVersion\":1}', now())", event);
            assertThatThrownBy(() -> db.update("INSERT INTO inventory_event_inbox "
                    + "VALUES ('a', ?, '{\"schemaVersion\":1}', now())", event)).isInstanceOf(DataAccessException.class);
            assertThatThrownBy(() -> db.update("INSERT INTO inventory_event_inbox "
                    + "VALUES ('c', ?, '{\"schemaVersion\":1}', NULL)", event)).isInstanceOf(DataAccessException.class);
        });
    }

    private void assertInvalid(JdbcTemplate db, UUID event, UUID order, UUID cause, String body) {
        assertThatThrownBy(() -> insert(db, event, order, cause, body)).isInstanceOf(DataAccessException.class);
    }

    private void insert(JdbcTemplate db, UUID event, UUID order, UUID cause, String body) {
        db.update("""
                INSERT INTO inventory_result_outbox
                (event_id, order_id, causation_id, event_type, schema_version, topic, message_key, payload)
                VALUES (?, ?, ?, 'InventoryRejected', 1, 'commerce.inventory.v1', ?, ?)
                """, event, order, cause, order.toString(), body);
    }

    private UUID seed(JdbcTemplate db, String state) {
        db.update("INSERT INTO stock VALUES ('APPLE', 10) ON CONFLICT DO NOTHING");
        UUID id = UUID.randomUUID();
        boolean rejected = state.equals("REJECTED");
        db.update("INSERT INTO inventory_reservation_attempts "
                + "(order_id, canonical_payload, outcome, unavailable_skus, created_at) "
                + "VALUES (?, '[{\"sku\":\"APPLE\",\"quantity\":2}]', ?, CAST(? AS jsonb), now())",
                id, rejected ? "REJECTED" : "RESERVED", rejected ? "{\"APPLE\":{\"requested\":2,\"available\":0}}" : null);
        if (!rejected) {
            db.update("INSERT INTO inventory_reservations VALUES (?, ?, now(), "
                    + (state.equals("RELEASED") ? "now()" : "NULL") + ")", id, state);
            db.update("INSERT INTO inventory_reservation_lines VALUES (?, 'APPLE', 2, 0)", id);
        }
        return id;
    }

    private Object snapshot(JdbcTemplate db) {
        return List.of(db.queryForList("SELECT * FROM inventory_reservation_attempts ORDER BY order_id"),
                db.queryForList("SELECT * FROM inventory_reservations ORDER BY order_id"),
                db.queryForList("SELECT * FROM inventory_reservation_lines ORDER BY order_id"),
                db.queryForList("SELECT * FROM stock ORDER BY sku"));
    }

    private void migrate(String schema, String target) {
        Flyway.configure().dataSource(dataSource).schemas(schema).defaultSchema(schema)
                .locations("classpath:db/migration").target(target).load().migrate();
    }

    private void inSchema(SchemaCheck check) throws Exception {
        String schema = "intake_upgrade_" + UUID.randomUUID().toString().replace("-", "");
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

    @FunctionalInterface
    interface SchemaCheck {
        void run(String schema, JdbcTemplate db) throws Exception;
    }
}
