package com.commercelab.inventory.persistence;

import com.commercelab.inventory.domain.Availability;
import com.commercelab.inventory.domain.ReservationLine;
import com.commercelab.inventory.service.ReservationPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
@Transactional(propagation = Propagation.MANDATORY)
public class ReservationAttemptStore {
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;

    public ReservationAttemptStore(JdbcTemplate jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    public boolean claim(UUID orderId, ReservationPayload payload, Instant createdAt) {
        return jdbc.update("""
                INSERT INTO inventory_reservation_attempts (order_id, canonical_payload, created_at)
                VALUES (?, CAST(? AS jsonb), ?)
                ON CONFLICT (order_id) DO NOTHING
                """, orderId, payload.canonicalJson().toString(), Timestamp.from(createdAt)) == 1;
    }

    public Optional<Attempt> find(UUID orderId) {
        return jdbc.query("""
                SELECT canonical_payload, outcome, unavailable_skus
                FROM inventory_reservation_attempts WHERE order_id = ?
                """, (rs, row) -> {
                    String outcome = rs.getString("outcome");
                    if (outcome == null) {
                        throw new IllegalStateException("unfinished reservation attempt: " + orderId);
                    }
                    // Normalize even migrated JSON: database collation is not Java String ordering.
                    ReservationPayload payload = new ReservationPayload(read(
                            rs.getString("canonical_payload"), new TypeReference<List<ReservationLine>>() {}));
                    Map<String, Availability> unavailable = "REJECTED".equals(outcome)
                            ? read(rs.getString("unavailable_skus"), new TypeReference<>() {}) : Map.of();
                    return new Attempt(payload, outcome, unavailable);
                }, orderId).stream().findFirst();
    }

    public void complete(UUID orderId, Map<String, Availability> unavailable) {
        boolean rejected = !unavailable.isEmpty();
        int updated = jdbc.update("""
                UPDATE inventory_reservation_attempts
                SET outcome = ?, unavailable_skus = CAST(? AS jsonb)
                WHERE order_id = ? AND outcome IS NULL
                """, rejected ? "REJECTED" : "RESERVED", rejected ? write(unavailable) : null, orderId);
        if (updated != 1) {
            throw new IllegalStateException("reservation attempt was not claimed: " + orderId);
        }
    }

    private <T> T read(String value, TypeReference<T> type) {
        try {
            return json.readValue(value, type);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("invalid stored reservation attempt JSON", exception);
        }
    }

    private String write(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("cannot serialize reservation attempt", exception);
        }
    }

    public record Attempt(ReservationPayload payload, String outcome, Map<String, Availability> unavailable) {}
}
