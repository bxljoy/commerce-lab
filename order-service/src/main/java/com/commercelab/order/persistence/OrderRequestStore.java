package com.commercelab.order.persistence;

import com.commercelab.order.service.OrderPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Repository;
import org.springframework.jdbc.core.JdbcTemplate;

/** Request identity operations participate in the creation service's transaction. */
@Repository
public class OrderRequestStore {
    public static final String KEY_CONSTRAINT = "pk_order_requests_key";
    private final EntityManager entityManager;
    private final ObjectMapper mapper;
    private final JdbcTemplate jdbc;

    public OrderRequestStore(EntityManager entityManager, ObjectMapper mapper, JdbcTemplate jdbc) {
        this.entityManager = entityManager;
        this.mapper = mapper;
        this.jdbc = jdbc;
    }

    public Optional<StoredRequest> find(String key) {
        var rows = entityManager.createNativeQuery("""
                SELECT order_id, CAST(canonical_payload AS text), fingerprint_version
                FROM order_requests WHERE request_key = :key
                """).setParameter("key", key).getResultList();
        if (rows.isEmpty()) return Optional.empty();
        Object[] row = (Object[]) rows.getFirst();
        try {
            return Optional.of(new StoredRequest((UUID) row[0], mapper.readTree((String) row[1]),
                    ((Number) row[2]).intValue()));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Invalid stored order request", ex);
        }
    }

    public void insert(String key, UUID orderId, OrderPayload payload, String correlationId) {
        // Flush persist-based aggregate inserts before enforcing the request foreign key.
        entityManager.flush();
        // JdbcTemplate shares the JPA transaction's connection and does not log raw
        // duplicate-key details as Hibernate's SqlExceptionHelper would.
        jdbc.update("""
                INSERT INTO order_requests
                    (request_key, order_id, canonical_payload, fingerprint, fingerprint_version, correlation_id)
                VALUES (?, ?, CAST(? AS jsonb), ?, 1, ?)
                """, key, orderId, payload.canonicalJson().toString(), payload.fingerprint(), correlationId);
    }

    public record StoredRequest(UUID orderId, JsonNode canonicalPayload, int fingerprintVersion) {}
}
