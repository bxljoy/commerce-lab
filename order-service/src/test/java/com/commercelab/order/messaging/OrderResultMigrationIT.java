package com.commercelab.order.messaging;

import static org.assertj.core.api.Assertions.*;

import com.commercelab.order.AbstractPostgresIntegrationTest;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

@SpringBootTest
class OrderResultMigrationIT extends AbstractPostgresIntegrationTest {
    @Autowired DataSource dataSource;
    @Autowired JdbcTemplate jdbc;

    @Test void upgradesV5WithPendingOrdersAndImmediateAcceptanceConstraints() throws Exception {
        String schema = "result_upgrade_" + UUID.randomUUID().toString().replace("-", "");
        jdbc.execute("CREATE SCHEMA " + schema);
        try (var connection = dataSource.getConnection()) {
            String previous = connection.getSchema();
            try {
                migrate(schema, "5");
                connection.setSchema(schema);
                var db = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
                UUID order = UUID.randomUUID(), cause = UUID.randomUUID(), result = UUID.randomUUID();
                db.update("INSERT INTO orders(id,customer_id,status,currency,placed_at) "
                        + "VALUES (?,'legacy','PENDING_INVENTORY','EUR',now())", order);
                db.update("INSERT INTO order_lines(id,order_id,sku,quantity,unit_price,line_position) "
                        + "VALUES (?,?,'A',1,1,0)", UUID.randomUUID(), order);
                db.update("INSERT INTO order_requests(request_key,order_id,canonical_payload,fingerprint,fingerprint_version) "
                        + "VALUES (?,?,'{}','proof',1)", order.toString(), order);
                db.update("INSERT INTO order_outbox(event_id,order_id,event_type,schema_version,topic,message_key,payload) "
                        + "VALUES (?,?,'OrderPlaced',1,'commerce.orders.v1',?,?)", cause, order, order.toString(),
                        "{\"eventId\":\"" + cause + "\",\"orderId\":\"" + order + "\",\"eventType\":\"OrderPlaced\",\"schemaVersion\":1}");
                var before = snapshot(db);
                migrate(schema, "6");
                assertThat(snapshot(db)).isEqualTo(before);
                assertThat(db.queryForObject("SELECT count(*) FROM order_event_inbox", Long.class)).isZero();
                assertThat(db.queryForObject("SELECT count(*) FROM order_inventory_results", Long.class)).isZero();
                assertThat(db.queryForObject("SELECT count(*) FROM pg_constraint c JOIN pg_namespace n "
                        + "ON c.connamespace=n.oid WHERE n.nspname=? AND c.condeferrable", Long.class, schema)).isZero();
                assertThatThrownBy(() -> accept(db, order, cause, result))
                        .isInstanceOf(DataIntegrityViolationException.class);
                assertThatThrownBy(() -> db.update("INSERT INTO order_event_inbox(consumer_name,event_id,content) "
                        + "VALUES ('order-inventory-result-v1',?,'[]')", result))
                        .isInstanceOf(DataIntegrityViolationException.class);
                db.update("INSERT INTO order_event_inbox(consumer_name,event_id,content) "
                        + "VALUES ('order-inventory-result-v1',?,'{}')", result);
                assertThatThrownBy(() -> accept(db, order, UUID.randomUUID(), result))
                        .isInstanceOf(DataIntegrityViolationException.class);
                assertThatThrownBy(() -> accept(db, UUID.randomUUID(), cause, result))
                        .isInstanceOf(DataIntegrityViolationException.class);
                accept(db, order, cause, result);
                assertThatThrownBy(() -> accept(db, order, cause, result))
                        .isInstanceOf(DataIntegrityViolationException.class);
                for (String assignment : List.of("consumer_name='wrong'", "result_type='Other'"))
                    assertThatThrownBy(() -> db.update("UPDATE order_inventory_results SET " + assignment))
                            .isInstanceOf(DataIntegrityViolationException.class);
                assertThat(db.queryForList("SELECT version FROM flyway_schema_history ORDER BY installed_rank", String.class))
                        .containsExactly("1", "2", "3", "4", "5", "6");
            } finally {
                connection.setSchema(previous);
            }
        } finally {
            jdbc.execute("DROP SCHEMA " + schema + " CASCADE");
        }
    }

    private void accept(JdbcTemplate db, UUID order, UUID cause, UUID result) {
        db.update("INSERT INTO order_inventory_results(order_id,causation_id,result_event_id,result_type) "
                + "VALUES (?,?,?,'InventoryReserved')", order, cause, result);
    }

    private Object snapshot(JdbcTemplate db) {
        return List.of(db.queryForList("SELECT * FROM orders"), db.queryForList("SELECT * FROM order_lines"),
                db.queryForList("SELECT * FROM order_requests"), db.queryForList("SELECT * FROM order_outbox"));
    }

    private void migrate(String schema, String version) {
        Flyway.configure().dataSource(dataSource).schemas(schema).defaultSchema(schema)
                .locations("classpath:db/migration").target(version).load().migrate();
    }
}
