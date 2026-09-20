package com.commercelab.inventory.outbox;

import com.commercelab.inventory.events.InventoryResult;
import java.sql.Timestamp;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
@Transactional(propagation = Propagation.MANDATORY)
public class InventoryResultStore {
    private final JdbcTemplate jdbc;

    public InventoryResultStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(InventoryResult result, String payload) {
        jdbc.update("""
                INSERT INTO inventory_result_outbox
                (event_id, order_id, causation_id, event_type, schema_version, topic, message_key, payload, created_at)
                VALUES (?, ?, ?, ?, ?, 'commerce.inventory.v1', ?, ?, ?)
                """, result.eventId(), result.orderId(), result.causationId(), result.eventType(),
                result.schemaVersion(), result.orderId().toString(), payload, Timestamp.from(result.occurredAt()));
    }
}
