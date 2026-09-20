package com.commercelab.order.messaging;

import com.commercelab.order.events.EventJson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryResultListener {
    private static final Logger LOG = LoggerFactory.getLogger(InventoryResultListener.class);
    private final OrderResultHandler handler;
    private final EventJson events;
    private final ConsumerCommitHook hook;
    private final PartitionFailureHandler failures;

    public InventoryResultListener(OrderResultHandler handler, EventJson events,
            ConsumerCommitHook hook, PartitionFailureHandler failures) {
        this.handler = handler;
        this.events = events;
        this.hook = hook;
        this.failures = failures;
    }

    @KafkaListener(id = "order-workflow", topics = ConsumerConfiguration.TOPIC,
            groupId = ConsumerConfiguration.GROUP, containerFactory = "workflowKafkaListenerContainerFactory",
            autoStartup = "${order.events.enabled:true}")
    public void onRecord(ConsumerRecord<String, String> record) {
        var previous = MDC.getCopyOfContextMap();
        try {
            MDC.clear();
            var event = events.readResult(record.key(), record.value());
            MDC.put("eventId", event.eventId().toString());
            MDC.put("orderId", event.orderId().toString());
            MDC.put("correlationId", event.correlationId());
            var outcome = handler.handle(record.key(), record.value());
            // The injected application handler is a Spring transaction proxy: return means committed.
            hook.afterDatabaseCommit(event.eventId(), outcome);
            failures.succeeded(record, outcome);
            LOG.info("consumer={} eventId={} orderId={} outcome={}",
                    ConsumerConfiguration.GROUP, event.eventId(), event.orderId(), outcome);
        } finally {
            if (previous == null) MDC.clear(); else MDC.setContextMap(previous);
        }
    }
}
