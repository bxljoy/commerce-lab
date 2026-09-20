package com.commercelab.order.messaging;

import com.commercelab.order.events.EventJson;
import com.commercelab.order.persistence.OrderCompletionStore;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderResultHandler {
    private final EventJson events;
    private final InboxStore inbox;
    private final OrderCompletionStore completion;

    public OrderResultHandler(EventJson events, InboxStore inbox, OrderCompletionStore completion) {
        this.events = events;
        this.inbox = inbox;
        this.completion = completion;
    }

    @Transactional
    public ProcessingOutcome handle(String key, String value) {
        var result = events.readResult(key, value);
        if (!inbox.claim("order-inventory-result-v1", result.eventId(), events.content(value)))
            return ProcessingOutcome.DUPLICATE;
        completion.complete(result);
        return ProcessingOutcome.APPLIED;
    }
}
