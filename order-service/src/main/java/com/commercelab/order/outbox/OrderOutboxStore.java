package com.commercelab.order.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Inserts participate in the caller's JPA/JDBC transaction; never create one here. */
@Repository
public class OrderOutboxStore {
    private final EntityManager entityManager;
    private final ObjectMapper mapper;
    private final JdbcTemplate jdbc;

    public OrderOutboxStore(EntityManager entityManager, ObjectMapper mapper, JdbcTemplate jdbc) {
        this.entityManager = entityManager;
        this.mapper = mapper;
        this.jdbc = jdbc;
    }

    /** JDBC-only read for completion commands; does not load or flush JPA entities. */
    public Optional<String> findOrderPlacedPayload(UUID orderId) {
        return jdbc.query("SELECT payload FROM order_outbox WHERE order_id = ? AND event_type = 'OrderPlaced'",
                rs -> rs.next() ? Optional.of(rs.getString(1)) : Optional.empty(), orderId);
    }

    public void insert(OutboxMessage message) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Outbox insert requires an active transaction");
        }
        validateEnvelope(message);
        // Flush the new aggregate before JDBC enforces its foreign key, as OrderRequestStore does.
        entityManager.flush();
        jdbc.update("""
                INSERT INTO order_outbox
                    (event_id, order_id, event_type, schema_version, topic, message_key, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, message.eventId(), message.orderId(), OrderPlacedEventFactory.EVENT_TYPE,
                OrderPlacedEventFactory.SCHEMA_VERSION, message.topic(), message.messageKey(), message.payload());
    }

    private void validateEnvelope(OutboxMessage message) {
        if (message == null || message.eventId() == null || message.orderId() == null
                || message.payload() == null || !OrderPlacedEventFactory.TOPIC.equals(message.topic())
                || !message.orderId().toString().equals(message.messageKey())) {
            throw new IllegalArgumentException("Invalid OrderPlaced message metadata");
        }
        try {
            JsonNode body = mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .readTree(message.payload());
            if (body == null || !body.isObject()
                    || !textEquals(body, "eventId", message.eventId().toString())
                    || !textEquals(body, "orderId", message.orderId().toString())
                    || !textEquals(body, "eventType", OrderPlacedEventFactory.EVENT_TYPE)
                    || !body.path("schemaVersion").isIntegralNumber()
                    || !body.path("schemaVersion").canConvertToInt()
                    || body.path("schemaVersion").intValue() != OrderPlacedEventFactory.SCHEMA_VERSION) {
                throw new IllegalArgumentException("OrderPlaced envelope does not match message metadata");
            }
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Invalid OrderPlaced JSON", ex);
        }
    }

    private boolean textEquals(JsonNode body, String field, String expected) {
        return body.path(field).isTextual() && expected.equals(body.path(field).textValue());
    }
}
