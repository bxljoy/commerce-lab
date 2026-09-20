package com.commercelab.inventory.messaging;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicIntegerArray;

public final class ConsumerMetrics {
    private final MeterRegistry meters;
    private final AtomicIntegerArray blocked = new AtomicIntegerArray(3);

    public ConsumerMetrics(MeterRegistry meters) {
        this.meters = meters;
        for (int p = 0; p < 3; p++) {
            final int partition = p;
            io.micrometer.core.instrument.Gauge.builder("consumer.blocked.partitions", blocked,
                    values -> values.get(partition))
                    .tags("consumer", ConsumerConfiguration.GROUP, "topic", ConsumerConfiguration.TOPIC,
                            "partition", Integer.toString(p)).register(meters);
        }
    }

    void outcome(int partition, String outcome, String code) {
        if (partition < 0 || partition >= 3) return;
        meters.counter("consumer." + outcome, "consumer", ConsumerConfiguration.GROUP,
                "topic", ConsumerConfiguration.TOPIC, "partition", Integer.toString(partition), "code", code).increment();
    }

    void blocked(int partition, boolean value) {
        if (partition >= 0 && partition < 3) blocked.set(partition, value ? 1 : 0);
    }

    void infrastructure() {
        meters.counter("consumer.infrastructure.failures", "consumer", ConsumerConfiguration.GROUP,
                "code", "INFRASTRUCTURE").increment();
    }
}
