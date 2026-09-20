package com.commercelab.inventory.messaging;

import com.commercelab.inventory.events.EventProtocolException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.transaction.CannotCreateTransactionException;

public final class PartitionFailureHandler implements CommonErrorHandler, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionFailureHandler.class);
    private final ConsumerMetrics metrics;
    private final ScheduledExecutorService scheduler;
    private final Map<TopicPartition, Assignment> assignments = new HashMap<>();
    private final ThreadLocal<ConsumerThread> consumerThread = new ThreadLocal<>();
    private boolean closed;

    public PartitionFailureHandler(ConsumerMetrics metrics) {
        this(metrics, Executors.newSingleThreadScheduledExecutor(task -> {
            var thread = new Thread(task, "workflow-partition-resume");
            thread.setDaemon(true);
            return thread;
        }));
    }

    PartitionFailureHandler(ConsumerMetrics metrics, ScheduledExecutorService scheduler) {
        this.metrics = metrics;
        this.scheduler = scheduler;
    }

    @Override public boolean isAckAfterHandle() { return false; }

    @Override
    public synchronized boolean handleOne(Exception exception, ConsumerRecord<?, ?> record,
            Consumer<?, ?> consumer, MessageListenerContainer container) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Assignment state = assignments.get(tp);
        // A lost assignment is replayed by its new owner from the committed offset.
        if (closed || state == null || state.owner != consumer) return true;
        long failedOffset = state.failedOffset == null ? record.offset() : Math.min(state.failedOffset, record.offset());
        consumer.seek(tp, failedOffset);
        state.container.pausePartition(tp);
        cancel(state);
        state.failedOffset = failedOffset;
        Throwable cause = exception;
        while (cause instanceof ListenerExecutionFailedException && cause.getCause() != null)
            cause = cause.getCause();
        boolean retry = cause instanceof TransientDataAccessException
                || cause instanceof DataAccessResourceFailureException
                || cause instanceof RecoverableDataAccessException
                || cause instanceof CannotCreateTransactionException
                || cause instanceof CommitFailedException
                || cause instanceof org.apache.kafka.common.errors.RetriableException;
        String code = cause instanceof EventProtocolException ? "PROTOCOL" : retry ? "TRANSIENT" : "UNEXPECTED";
        metrics.blocked(tp.partition(), true);
        metrics.outcome(tp.partition(), retry ? "retry" : "blocked", code);
        metadataLog(tp, failedOffset, code);
        if (retry) {
            long delay = state.nextDelay;
            state.nextDelay = Math.min(30, delay * 2);
            Object generation = new Object();
            state.generation = generation;
            state.resume = scheduler.schedule(() -> resume(tp, state, generation), delay, TimeUnit.SECONDS);
        }
        return true;
    }

    @Override
    public void handleRemaining(Exception exception, List<ConsumerRecord<?, ?>> records,
            Consumer<?, ?> consumer, MessageListenerContainer container) {
        // Kafka 3.2.4 routes CommitFailedException here even with seeksAfterHandling=false.
        // Rewind every represented partition to its earliest uncommitted input.
        Map<TopicPartition, ConsumerRecord<?, ?>> first = new LinkedHashMap<>();
        for (var record : records) {
            var tp = new TopicPartition(record.topic(), record.partition());
            first.merge(tp, record, (a, b) -> a.offset() <= b.offset() ? a : b);
        }
        first.values().forEach(record -> handleOne(exception, record, consumer, container));
    }

    private synchronized void resume(TopicPartition tp, Assignment state, Object generation) {
        if (!closed && assignments.get(tp) == state && state.generation == generation) {
            state.generation = null;
            state.resume = null;
            state.container.resumePartition(tp);
        }
    }

    synchronized Assignment processingAssignment(ConsumerRecord<?, ?> record) {
        var state = assignments.get(new TopicPartition(record.topic(), record.partition()));
        var thread = consumerThread.get();
        return state != null && thread != null && state.owner == thread.owner
                && state.container == thread.container ? state : null;
    }

    synchronized void succeeded(ConsumerRecord<?, ?> record, ProcessingOutcome outcome, Assignment processing) {
        var tp = new TopicPartition(record.topic(), record.partition());
        Assignment state = assignments.get(tp);
        if (state != null && state == processing) {
            cancel(state);
            state.nextDelay = 1;
            state.failedOffset = null;
            metrics.blocked(tp.partition(), false);
        }
        metrics.outcome(tp.partition(), outcome == ProcessingOutcome.APPLIED ? "applied" : "duplicate", "OK");
    }

    // Called synchronously by the child's publisher, before application event dispatch.
    void consumerLifecycle(Object event) {
        if (event instanceof ConsumerStartingEvent starting) {
            consumerThread.set(new ConsumerThread(starting.getSource(MessageListenerContainer.class)));
        } else if (event instanceof ConsumerStoppedEvent stopped) {
            var thread = consumerThread.get();
            if (thread != null && thread.container == stopped.getSource(MessageListenerContainer.class))
                consumerThread.remove();
        }
    }

    public ConsumerAwareRebalanceListener rebalanceListener() {
        return new ConsumerAwareRebalanceListener() {
            @Override public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                assigned(consumer, partitions);
            }
            @Override public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer,
                    Collection<TopicPartition> partitions) { revoked(consumer, partitions); }
            @Override public void onPartitionsLost(Consumer<?, ?> consumer,
                    Collection<TopicPartition> partitions) { revoked(consumer, partitions); }
        };
    }

    private synchronized void assigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        var thread = consumerThread.get();
        if (thread == null) throw new IllegalStateException("Missing consumer thread binding");
        thread.owner = consumer;
        for (var tp : partitions) {
            Assignment old = assignments.get(tp);
            if (old != null && old.owner == consumer) continue; // Cooperative retained ownership.
            if (old != null) {
                cancel(old);
                old.container.resumePartition(tp);
            }
            // A previous child can still advertise this partition after poll timeout.
            // Only the publishing consumer thread identifies the actual child.
            assignments.put(tp, new Assignment(consumer, thread.container));
            metrics.blocked(tp.partition(), false);
        }
    }

    private synchronized void revoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        for (var tp : partitions) {
            Assignment state = assignments.get(tp);
            if (state != null && state.owner == consumer) {
                cancel(state);
                assignments.remove(tp);
                metrics.blocked(tp.partition(), false);
                // Spring retains pause requests across assignment unless explicitly cleared.
                state.container.resumePartition(tp);
            }
        }
    }

    @Override public void handleOtherException(Exception exception, Consumer<?, ?> consumer,
            MessageListenerContainer container, boolean batchListener) {
        metrics.infrastructure();
        var previous = MDC.getCopyOfContextMap();
        try {
            MDC.clear();
            LOG.warn("consumer={} code=INFRASTRUCTURE", ConsumerConfiguration.GROUP);
        } finally { restoreMdc(previous); }
    }

    private void metadataLog(TopicPartition tp, long offset, String code) {
        var previous = MDC.getCopyOfContextMap();
        try {
            MDC.clear();
            LOG.warn("consumer={} topic={} partition={} offset={} code={}",
                    ConsumerConfiguration.GROUP, tp.topic(), tp.partition(), offset, code);
        } finally { restoreMdc(previous); }
    }

    private static void restoreMdc(Map<String, String> previous) {
        if (previous == null) MDC.clear(); else MDC.setContextMap(previous);
    }

    private static void cancel(Assignment state) {
        state.generation = null;
        if (state.resume != null) state.resume.cancel(false);
        state.resume = null;
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        assignments.forEach((tp, state) -> {
            cancel(state);
            state.container.resumePartition(tp);
            metrics.blocked(tp.partition(), false);
        });
        assignments.clear();
        scheduler.shutdownNow();
    }

    private static final class ConsumerThread {
        final MessageListenerContainer container;
        Consumer<?, ?> owner;
        ConsumerThread(MessageListenerContainer container) { this.container = container; }
    }

    static final class Assignment {
        final Consumer<?, ?> owner;
        final MessageListenerContainer container;
        Long failedOffset;
        long nextDelay = 1;
        Object generation;
        ScheduledFuture<?> resume;
        Assignment(Consumer<?, ?> owner, MessageListenerContainer container) {
            this.owner = owner;
            this.container = container;
        }
    }
}
