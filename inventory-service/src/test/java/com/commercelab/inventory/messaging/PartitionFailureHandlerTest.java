package com.commercelab.inventory.messaging;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import com.commercelab.inventory.events.EventProtocolException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.kafka.listener.*;
import org.slf4j.MDC;

class PartitionFailureHandlerTest {
    final TopicPartition p0 = new TopicPartition(ConsumerConfiguration.TOPIC, 0);
    final ConsumerRecord<String, String> record = new ConsumerRecord<>(p0.topic(), 0, 4, "private-key", "sensitive-body");
    final Consumer<?, ?> consumer = mock(Consumer.class);
    final MessageListenerContainer container = mock(MessageListenerContainer.class);
    final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    final ConsumerMetrics metrics = new ConsumerMetrics(meters);
    final PartitionFailureHandler handler = new PartitionFailureHandler(metrics, scheduler);

    @BeforeEach void assigned() {
        handler.consumerLifecycle(new org.springframework.kafka.event.ConsumerStartingEvent(container, container));
        when(scheduler.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.SECONDS)))
                .thenReturn(mock(ScheduledFuture.class));
        handler.rebalanceListener().onPartitionsAssigned(consumer, List.of(p0));
    }
    @AfterEach void close() { handler.close(); MDC.clear(); }

    @Test void poisonSeeksPausesOnlyItsPartitionAndNeverAcknowledges() {
        assertThat(handler.handleOne(new ListenerExecutionFailedException("wrapper",
                new EventProtocolException("INVALID_JSON")), record, consumer, container)).isTrue();
        verify(consumer).seek(p0, 4);
        verify(container).pausePartition(p0);
        verify(container, never()).pause();
        verify(consumer, never()).commitSync();
        assertThat(handler.isAckAfterHandle()).isFalse();
        assertThat(handler.seeksAfterHandling()).isFalse();
        assertThat(blocked()).isEqualTo(1);
        verifyNoInteractions(scheduler);
    }

    @Test void commitFailedRemainingPathRewindsEarliestOffsetWithoutSkip() {
        handler.handleRemaining(new CommitFailedException("sensitive-body"),
                List.of(record, new ConsumerRecord<>(p0.topic(), 0, 5, "key", "later")), consumer, container);
        verify(consumer).seek(p0, 4);
        verify(consumer, never()).seek(p0, 5);
        verify(container).pausePartition(p0);
        verify(consumer, never()).commitSync();
        verify(scheduler).schedule(any(Runnable.class), eq(1L), eq(TimeUnit.SECONDS));
        assertThat(handler.isAckAfterHandle()).isFalse();
    }

    @Test void repeatedFailureCannotMoveTheFailedOffsetForward() {
        handler.handleOne(new CommitFailedException(), record, consumer, container);
        handler.handleOne(new CommitFailedException(),
                new ConsumerRecord<>(p0.topic(), 0, 5, "key", "later"), consumer, container);
        verify(consumer, times(2)).seek(p0, 4);
        verify(consumer, never()).seek(p0, 5);
    }

    @Test void retryIsNonblockingAndCappedAndSuccessResetsIt() {
        for (int i = 0; i < 70; i++)
            handler.handleOne(new TransientDataAccessResourceException("private"), record, consumer, container);
        var delays = ArgumentCaptor.forClass(Long.class);
        verify(scheduler, times(70)).schedule(any(Runnable.class), delays.capture(), eq(TimeUnit.SECONDS));
        assertThat(delays.getAllValues().subList(0, 7)).containsExactly(1L, 2L, 4L, 8L, 16L, 30L, 30L);
        assertThat(delays.getAllValues()).allMatch(n -> n > 0 && n <= 30);
        handler.succeeded(record, ProcessingOutcome.DUPLICATE, handler.processingAssignment(record));
        assertThat(blocked()).isZero();
        assertThat(meters.get("consumer.duplicate").counter().count()).isEqualTo(1);
        handler.handleOne(new TransientDataAccessResourceException("private"), record, consumer, container);
        verify(scheduler, times(2)).schedule(any(Runnable.class), eq(1L), eq(TimeUnit.SECONDS));
    }

    @Test void revokeAndLostClearPauseRequestsAndInvalidateAlreadyRunningTimers() {
        handler.handleOne(new TransientDataAccessResourceException("private"), record, consumer, container);
        var tasks = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).schedule(tasks.capture(), eq(1L), eq(TimeUnit.SECONDS));
        handler.rebalanceListener().onPartitionsRevokedBeforeCommit(consumer, List.of(p0));
        verify(container).resumePartition(p0);
        assertThat(blocked()).isZero();
        handler.rebalanceListener().onPartitionsAssigned(consumer, List.of(p0));
        handler.handleOne(new EventProtocolException("INVALID_JSON"), record, consumer, container);
        tasks.getValue().run();
        verify(container, times(1)).resumePartition(p0);
        assertThat(blocked()).isEqualTo(1);
        handler.rebalanceListener().onPartitionsLost(consumer, List.of(p0));
        verify(container, times(2)).resumePartition(p0);
        assertThat(blocked()).isZero();
    }

    @Test void retryTaskUsesContainerOnlyAndOldTaskCannotUnblockNewFailure() {
        handler.handleOne(new TransientDataAccessResourceException("private"), record, consumer, container);
        var task = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).schedule(task.capture(), eq(1L), eq(TimeUnit.SECONDS));
        clearInvocations(consumer);
        task.getValue().run();
        verify(container).resumePartition(p0);
        verifyNoInteractions(consumer);
        handler.handleOne(new EventProtocolException("INVALID_JSON"), record, consumer, container);
        task.getValue().run();
        verify(container, times(1)).resumePartition(p0);
    }

    @Test void unexpectedAndInfrastructureFailuresAreMetadataOnlyAndRestoreMdc() {
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(PartitionFailureHandler.class);
        var logs = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        logs.start(); logger.addAppender(logs);
        MDC.put("request", "existing");
        try {
            handler.handleOne(new IllegalStateException("sensitive-body"), record, consumer, container);
            handler.handleOtherException(new IllegalStateException("sensitive-body"), consumer, container, false);
            assertThat(logs.list).isNotEmpty().allSatisfy(e -> {
                assertThat(e.getFormattedMessage()).doesNotContain("sensitive-body", "private-key");
                assertThat(e.getThrowableProxy()).isNull();
            });
            assertThat(MDC.getCopyOfContextMap()).containsExactlyEntriesOf(java.util.Map.of("request", "existing"));
            assertThat(blocked()).isEqualTo(1);
            verifyNoInteractions(scheduler);
            assertThat(meters.get("consumer.infrastructure.failures").counter().count()).isEqualTo(1);
        } finally { logger.detachAppender(logs); logs.stop(); }
    }

    @Test void revocationClearsTheOwningChildWithoutTakingTheParentLifecycleLock() {
        handler.rebalanceListener().onPartitionsRevokedBeforeCommit(consumer, List.of(p0));
        var child = mock(MessageListenerContainer.class);
        handler.consumerLifecycle(new org.springframework.kafka.event.ConsumerStartingEvent(child, container));
        handler.rebalanceListener().onPartitionsAssigned(consumer, List.of(p0));
        clearInvocations(container);
        handler.handleOne(new EventProtocolException("INVALID_JSON"), record, consumer, container);
        verify(child).pausePartition(p0);
        handler.rebalanceListener().onPartitionsRevokedBeforeCommit(consumer, List.of(p0));
        verify(child).resumePartition(p0);
        verify(container, never()).pausePartition(any());
        verify(container, never()).resumePartition(any());
    }

    @Test void closeCancelsResourcesAndClearsBlockedState() {
        handler.handleOne(new TransientDataAccessResourceException("private"), record, consumer, container);
        handler.close();
        verify(scheduler).shutdownNow();
        assertThat(blocked()).isZero();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void lateSuccessCannotClearNewOwnersBlock(boolean transientFailure) {
        var processing = handler.processingAssignment(record);
        var nextConsumer = mock(Consumer.class);
        var nextChild = mock(MessageListenerContainer.class);
        handler.consumerLifecycle(new org.springframework.kafka.event.ConsumerStartingEvent(nextChild, container));
        handler.rebalanceListener().onPartitionsAssigned(nextConsumer, List.of(p0));
        Exception failure = transientFailure ? new CommitFailedException() : new EventProtocolException("INVALID_JSON");
        handler.handleOne(failure, record, nextConsumer, nextChild);
        handler.succeeded(record, ProcessingOutcome.APPLIED, processing);
        assertThat(blocked()).isEqualTo(1);
        if (transientFailure) {
            var task = ArgumentCaptor.forClass(Runnable.class);
            verify(scheduler).schedule(task.capture(), eq(1L), eq(TimeUnit.SECONDS));
            task.getValue().run();
            verify(nextChild).resumePartition(p0);
        }
        handler.handleOne(failure, new ConsumerRecord<>(p0.topic(), 0, 5, "key", "later"),
                nextConsumer, nextChild);
        verify(nextConsumer, times(2)).seek(p0, 4);
        verify(nextConsumer, never()).seek(p0, 5);
    }

    private double blocked() {
        return meters.get("consumer.blocked.partitions").tag("partition", "0").gauge().value();
    }
}
