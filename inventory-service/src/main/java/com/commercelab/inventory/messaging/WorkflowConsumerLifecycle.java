package com.commercelab.inventory.messaging;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

/** Owns initial listener startup without coupling HTTP availability to broker DNS. */
public final class WorkflowConsumerLifecycle implements SmartLifecycle, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(WorkflowConsumerLifecycle.class);
    private final KafkaListenerEndpointRegistry registry;
    private final String listenerId;
    private final ScheduledExecutorService worker;
    private volatile boolean running;
    private boolean closed;
    private ScheduledFuture<?> retry;
    // Only the serial worker touches these; callbacks only complete the future.
    private boolean started;
    private CompletableFuture<Void> cleanup = CompletableFuture.completedFuture(null);

    WorkflowConsumerLifecycle(KafkaListenerEndpointRegistry registry, String listenerId) {
        this(registry, listenerId, Executors.newSingleThreadScheduledExecutor(task -> {
            var thread = new Thread(task, listenerId + "-startup");
            thread.setDaemon(true);
            return thread;
        }));
    }

    WorkflowConsumerLifecycle(KafkaListenerEndpointRegistry registry, String listenerId,
            ScheduledExecutorService worker) {
        this.registry = registry;
        this.listenerId = listenerId;
        this.worker = worker;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() { start(); }

    @Override public boolean isAutoStartup() { return false; }
    @Override public int getPhase() { return Integer.MAX_VALUE; }
    @Override public boolean isRunning() { return running; }

    @Override public synchronized void start() {
        if (closed || running) return;
        running = true;
        retry = worker.scheduleWithFixedDelay(this::attempt, 0, 5, TimeUnit.SECONDS);
    }

    private void attempt() {
        if (!running || started || !cleanup.isDone() || cleanup.isCompletedExceptionally()) return;
        var container = registry.getListenerContainer(listenerId);
        if (container == null) {
            LOG.error("listener={} startup=waiting code=CONTAINER_MISSING", listenerId);
            return;
        }
        try {
            container.start();
            started = true;
            LOG.info("listener={} startup=started", listenerId);
        } catch (RuntimeException failure) {
            LOG.warn("listener={} startup=retry code={}", listenerId, failure.getClass().getSimpleName());
            // Spring Kafka 3.2.4 sets the parent running flag before starting children.
            // Never trust that flag after a failed start, or retry until all children stop.
            cleanup = stopContainer(container);
        }
    }

    private CompletableFuture<Void> stopContainer(MessageListenerContainer container) {
        started = false;
        var stopped = new CompletableFuture<Void>();
        try {
            container.stop(() -> stopped.complete(null));
        } catch (RuntimeException failure) {
            stopped.completeExceptionally(failure);
            LOG.error("listener={} startup=blocked code=STOP_FAILED", listenerId);
        }
        return stopped;
    }

    @Override public void stop() { stop(() -> {}); }

    @Override public synchronized void stop(Runnable callback) {
        running = false;
        if (retry != null) retry.cancel(false);
        if (worker.isShutdown()) {
            callback.run();
            return;
        }
        // Queued after any in-flight start; shutdown cannot race a late successful start.
        worker.execute(() -> {
            if (cleanup.isDone()) {
                var container = registry.getListenerContainer(listenerId);
                if (container != null) cleanup = stopContainer(container);
            }
            cleanup.whenComplete((ignored, failure) -> callback.run());
        });
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        stop();
        worker.shutdown();
    }
}
