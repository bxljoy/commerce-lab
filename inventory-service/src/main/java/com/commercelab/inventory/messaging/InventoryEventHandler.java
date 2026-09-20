package com.commercelab.inventory.messaging;

import com.commercelab.inventory.events.EventJson;
import com.commercelab.inventory.outbox.InventoryResultFactory;
import com.commercelab.inventory.outbox.InventoryResultStore;
import com.commercelab.inventory.service.InventoryService;
import com.commercelab.inventory.service.ReserveInventoryCommand;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryEventHandler {
    private final EventJson events;
    private final InboxStore inbox;
    private final InventoryService inventory;
    private final InventoryResultFactory factory;
    private final InventoryResultStore results;

    public InventoryEventHandler(EventJson events, InboxStore inbox, InventoryService inventory,
            InventoryResultFactory factory, InventoryResultStore results) {
        this.events = events;
        this.inbox = inbox;
        this.inventory = inventory;
        this.factory = factory;
        this.results = results;
    }

    @Transactional
    public ProcessingOutcome handle(String key, String value) {
        var event = events.readOrderPlaced(key, value);
        if (!inbox.claim("inventory-order-placed-v1", event.eventId(), events.content(value)))
            return ProcessingOutcome.DUPLICATE;
        var command = new ReserveInventoryCommand(event.orderId(), event.lines().stream()
                .map(line -> new ReserveInventoryCommand.Line(line.sku(), line.quantity())).toList());
        var result = factory.create(event, inventory.reserve(command));
        results.insert(result, events.writeResult(result));
        return ProcessingOutcome.APPLIED;
    }
}
