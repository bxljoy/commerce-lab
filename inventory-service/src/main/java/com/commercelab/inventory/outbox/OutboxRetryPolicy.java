package com.commercelab.inventory.outbox;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntSupplier;
import org.springframework.stereotype.Component;

@Component
public class OutboxRetryPolicy {
    private final IntSupplier jitter;

    public OutboxRetryPolicy() {
        this(() -> ThreadLocalRandom.current().nextInt(251));
    }

    public OutboxRetryPolicy(IntSupplier jitter) {
        this.jitter = jitter;
    }

    public Duration delay(long attemptCount) {
        if (attemptCount < 1) {
            throw new IllegalArgumentException("Attempt count must be positive");
        }
        // Cap before shifting: even Long.MAX_VALUE attempts cannot overflow.
        long baseMillis = attemptCount >= 7 ? 60_000 : 1_000L << (attemptCount - 1);
        return Duration.ofMillis(baseMillis + jitter.getAsInt());
    }
}
