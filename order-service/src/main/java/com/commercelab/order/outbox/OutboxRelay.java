package com.commercelab.order.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private final OutboxDeliveryStore store;
    private final OutboxRetryPolicy retryPolicy;
    private final OutboxPublisher publisher;
    private final OutboxPublicationHook hook;
    private final OutboxProperties properties;
    private final OutboxMetrics metrics;
    private final ObjectMapper mapper;
    private final AtomicBoolean running = new AtomicBoolean();

    public OutboxRelay(OutboxDeliveryStore store, OutboxRetryPolicy retryPolicy, OutboxPublisher publisher,
            OutboxPublicationHook hook, OutboxProperties properties, OutboxMetrics metrics, ObjectMapper mapper) {
        this.store = store;
        this.retryPolicy = retryPolicy;
        this.publisher = publisher;
        this.hook = hook;
        this.properties = properties;
        this.metrics = metrics;
        this.mapper = mapper;
    }

    /** Returns claims attempted, not deliveries. A lease can recover any interrupted bookkeeping. */
    public int runOnce() {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Outbox relay cannot run inside a transaction");
        }
        if (!running.compareAndSet(false, true)) return 0;
        Map<String, String> previousMdc = MDC.getCopyOfContextMap();
        int attempts = 0;
        try {
            while (attempts < properties.maxAttemptsPerPass() && !Thread.currentThread().isInterrupted()) {
                OutboxClaim claim;
                try {
                    var next = store.claimNext(Duration.ofMillis(properties.leaseMs()));
                    if (next.isEmpty()) break;
                    claim = next.get();
                } catch (RuntimeException ex) {
                    metrics.claimFailed();
                    log.warn("Outbox outcome=bookkeeping_failed code=CLAIM_FAILED");
                    break;
                }
                attempts++;
                if (!attempt(claim)) break;
            }
            return attempts;
        } finally {
            if (previousMdc == null) MDC.clear();
            else MDC.setContextMap(previousMdc);
            running.set(false);
        }
    }

    private boolean attempt(OutboxClaim claim) {
        long started = System.nanoTime();
        OutboxMessage message = claim.message();
        MDC.put("eventId", message.eventId().toString());
        MDC.put("orderId", message.orderId().toString());
        MDC.put("correlationId", correlationId(message));
        try {
            checkInterrupted();
            publisher.publish(message);
            checkInterrupted();
        } catch (InterruptedException ex) {
            return interrupted(claim, started);
        } catch (RuntimeException ex) {
            if (Thread.currentThread().isInterrupted()) return interrupted(claim, started);
            String code = ex instanceof OutboxPublishException failure ? failure.code() : "SEND_FAILED";
            try {
                boolean updated = store.reschedule(message.eventId(), claim.token(), retryPolicy.delay(claim.attemptCount()), code);
                report(claim, started, updated ? "retry" : "stale", code);
                return true;
            } catch (RuntimeException bookkeepingFailure) {
                report(claim, started, "bookkeeping_failed", "RESCHEDULE_FAILED");
                return false;
            }
        }
        try {
            hook.afterAcknowledgement(message);
            checkInterrupted();
        } catch (InterruptedException ex) {
            return interrupted(claim, started);
        } catch (RuntimeException ex) {
            report(claim, started, "hook_failed", "HOOK_FAILED");
            return false;
        }
        try {
            boolean updated = store.markDelivered(message.eventId(), claim.token());
            report(claim, started, updated ? "delivered" : "stale", "NONE");
            return true;
        } catch (RuntimeException ex) {
            report(claim, started, "bookkeeping_failed", "MARK_FAILED");
            return false;
        }
    }

    private void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Relay interrupted");
    }

    private boolean interrupted(OutboxClaim claim, long started) {
        Thread.currentThread().interrupt();
        report(claim, started, "interrupted", "INTERRUPTED");
        return false;
    }

    private String correlationId(OutboxMessage message) {
        try {
            String id = mapper.readTree(message.payload()).path("correlationId").asText();
            return id.matches("[A-Za-z0-9._-]{1,128}") ? id : "unknown";
        } catch (com.fasterxml.jackson.core.JsonProcessingException | RuntimeException ex) {
            return "unknown";
        }
    }

    private void report(OutboxClaim claim, long started, String outcome, String code) {
        long elapsed = System.nanoTime() - started;
        metrics.recordAttempt(outcome, elapsed);
        log.info("Outbox eventId={} orderId={} correlationId={} attempt={} outcome={} latencyMs={} code={}",
                claim.message().eventId(), claim.message().orderId(), MDC.get("correlationId"),
                claim.attemptCount(), outcome, TimeUnit.NANOSECONDS.toMillis(elapsed), code);
    }
}
