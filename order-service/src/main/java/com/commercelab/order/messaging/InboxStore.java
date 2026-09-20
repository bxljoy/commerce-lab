package com.commercelab.order.messaging;

import com.commercelab.order.events.EventJson;
import com.commercelab.order.events.EventProtocolException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
@Transactional(propagation = Propagation.MANDATORY)
public class InboxStore {
    private final JdbcTemplate jdbc;
    private final EventJson events;

    public InboxStore(JdbcTemplate jdbc, EventJson events) {
        this.jdbc = jdbc;
        this.events = events;
    }

    public boolean claim(String consumer, UUID eventId, JsonNode content) {
        int inserted = jdbc.update("""
                INSERT INTO order_event_inbox(consumer_name, event_id, content, processed_at)
                VALUES (?, ?, CAST(? AS jsonb), clock_timestamp())
                ON CONFLICT (consumer_name, event_id) DO NOTHING
                """, consumer, eventId, content.toString());
        if (inserted == 1) return true;
        String stored = jdbc.queryForObject("""
                SELECT content::text FROM order_event_inbox WHERE consumer_name = ? AND event_id = ?
                """, String.class, consumer, eventId);
        if (!events.content(stored).equals(content)) throw new EventProtocolException("EVENT_IDENTITY_CONFLICT");
        return false;
    }
}
