package com.commercelab.order.outbox;

import com.commercelab.order.domain.Order;
import com.commercelab.order.web.CorrelationIdFilter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class OrderPlacedEventFactory {
    public static final String EVENT_TYPE = "OrderPlaced";
    public static final int SCHEMA_VERSION = 1;
    public static final String TOPIC = "commerce.orders.v1";

    private final Clock clock;
    private final ObjectMapper mapper;

    public OrderPlacedEventFactory(Clock clock, ObjectMapper mapper) {
        this.clock = clock;
        this.mapper = mapper;
    }

    public OutboxMessage create(Order order, String correlationId) {
        var event = new OrderPlacedEvent(UUID.randomUUID(), EVENT_TYPE, SCHEMA_VERSION, clock.instant(),
                order.id(), CorrelationIdFilter.validOrNew(correlationId), order.lines().stream()
                        .map(line -> new OrderPlacedEvent.Line(line.sku(), line.quantity())).toList());
        try {
            return new OutboxMessage(event.eventId(), order.id(), TOPIC, order.id().toString(),
                    mapper.writeValueAsString(event));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Cannot serialize OrderPlaced event", ex);
        }
    }
}
