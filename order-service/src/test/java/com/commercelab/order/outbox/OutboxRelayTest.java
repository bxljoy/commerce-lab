package com.commercelab.order.outbox;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;

class OutboxRelayTest {
    final OutboxDeliveryStore store = mock(OutboxDeliveryStore.class);
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    final OutboxClaim claim = new OutboxClaim(new OutboxMessage(UUID.randomUUID(), UUID.randomUUID(),
            "commerce.orders.v1", "key", "{\"correlationId\":\"test-correlation\",\"secret\":\"DO_NOT_LOG\"}"),
            UUID.randomUUID(), 3);

    @AfterEach void cleanup() {
        Thread.interrupted();
        MDC.clear();
        TransactionSynchronizationManager.clear();
        meters.close();
    }

    OutboxRelay relay(OutboxPublisher publisher, OutboxPublicationHook hook, int budget) {
        return new OutboxRelay(store, new OutboxRetryPolicy(() -> 0), publisher, hook,
                new OutboxProperties(1000, budget, 60000, 12000, 2000, 10000, 3000),
                new OutboxMetrics(store, meters), new ObjectMapper());
    }

    void oneClaim() {
        when(store.claimNext(any())).thenReturn(Optional.of(claim), Optional.empty());
        when(store.markDelivered(any(), any())).thenReturn(true);
    }

    @Test void acknowledgementAndHookPrecedeMarkingWithoutTransactionAndRestoreMdc() {
        oneClaim();
        MDC.put("correlationId", "caller");
        var stages = new java.util.ArrayList<String>();
        doAnswer(inv -> { stages.add("mark"); return true; }).when(store).markDelivered(any(), any());
        var relay = relay(message -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            verify(store, never()).markDelivered(any(), any());
            assertThat(message).isSameAs(claim.message());
            assertThat(MDC.get("correlationId")).isEqualTo("test-correlation");
            stages.add("ack");
        }, message -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            stages.add("hook");
        }, 20);
        assertThat(relay.runOnce()).isEqualTo(1);
        assertThat(stages).containsExactly("ack", "hook", "mark");
        assertThat(MDC.get("correlationId")).isEqualTo("caller");
        assertThat(meters.get("outbox.attempts").tag("outcome", "delivered").counter().count()).isEqualTo(1);
    }

    @Test void failureUsesClaimCountAndStableCodeAndKeepsWorking() {
        oneClaim();
        when(store.reschedule(any(), any(), any(), any())).thenReturn(true);
        var relay = relay(message -> { throw new OutboxPublishException("SEND_FAILED", new RuntimeException()); }, m -> {}, 20);
        assertThat(relay.runOnce()).isEqualTo(1);
        verify(store).reschedule(claim.message().eventId(), claim.token(), Duration.ofSeconds(4), "SEND_FAILED");
        verify(store, never()).markDelivered(any(), any());
        verify(store, times(2)).claimNext(any());
    }

    @Test void capsAttemptsNotSuccessfulDeliveriesAndEmptyQueueIsCheap() {
        when(store.claimNext(any())).thenReturn(Optional.of(claim));
        when(store.reschedule(any(), any(), any(), any())).thenReturn(true);
        var relay = relay(m -> { throw new OutboxPublishException("RECORD_TOO_LARGE", null); }, m -> {}, 2);
        assertThat(relay.runOnce()).isEqualTo(2);
        verify(store, times(2)).claimNext(Duration.ofSeconds(60));
        when(store.claimNext(any())).thenReturn(Optional.empty());
        assertThat(relay.runOnce()).isZero();
    }

    @Test void interruptRestoredLeaseLeftRecoverableAndGuardReleased() {
        oneClaim();
        var relay = relay(m -> { throw new InterruptedException("stop"); }, m -> {}, 20);
        assertThat(relay.runOnce()).isEqualTo(1);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        verify(store, never()).reschedule(any(), any(), any(), any());
        verify(store, never()).markDelivered(any(), any());
        verify(store, times(1)).claimNext(any());
        Thread.interrupted();
        assertThat(relay.runOnce()).isZero();
    }

    @Test void alreadyInterruptedPassDoesNotClaim() {
        Thread.currentThread().interrupt();
        assertThat(relay(m -> {}, m -> {}, 20).runOnce()).isZero();
        verifyNoInteractions(store);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    @Test void hookInterruptionDoesNotRecordAcknowledgedMessage() {
        oneClaim();
        assertThat(relay(m -> {}, m -> { throw new InterruptedException(); }, 20).runOnce()).isEqualTo(1);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        verify(store, never()).markDelivered(any(), any());
    }

    @Test void hookFailureLeavesLeaseAndStopsPass() {
        oneClaim();
        assertThat(relay(m -> {}, m -> { throw new IllegalStateException("hook failed"); }, 20).runOnce()).isEqualTo(1);
        verify(store, never()).markDelivered(any(), any());
        verify(store, never()).reschedule(any(), any(), any(), any());
        verify(store, times(1)).claimNext(any());
        assertThat(meters.get("outbox.attempts").tag("outcome", "hook_failed").counter().count()).isEqualTo(1);
    }

    @SuppressWarnings("unchecked")
    @Test void kafkaSynchronousInterruptAndInterruptedFutureStopWithoutRescheduling() throws Exception {
        oneClaim();
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        when(template.send(anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> { throw new org.apache.kafka.common.errors.InterruptException(new InterruptedException()); });
        var relay = relay(new KafkaOutboxPublisher(template, OutboxProperties.defaults()), m -> {}, 20);
        assertThat(relay.runOnce()).isEqualTo(1);
        assertThat(Thread.interrupted()).isTrue();
        oneClaim();
        CompletableFuture<SendResult<String, String>> future = mock(CompletableFuture.class);
        when(future.get(12000, TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException());
        doReturn(future).when(template).send(anyString(), anyString(), anyString());
        assertThat(relay.runOnce()).isEqualTo(1);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        verify(store, never()).reschedule(any(), any(), any(), any());
        verify(store, never()).markDelivered(any(), any());
    }

    @Test void staleSuccessAndFailureAreNotCountedAsDeliveryOrRetry() {
        oneClaim();
        when(store.markDelivered(any(), any())).thenReturn(false);
        assertThat(relay(m -> {}, m -> {}, 20).runOnce()).isEqualTo(1);
        assertThat(meters.get("outbox.attempts").tag("outcome", "stale").counter().count()).isEqualTo(1);
        assertThat(meters.find("outbox.attempts").tag("outcome", "delivered").counter()).isNull();
        oneClaim();
        assertThat(relay(m -> { throw new OutboxPublishException("SEND_FAILED", null); }, m -> {}, 20).runOnce()).isEqualTo(1);
        assertThat(meters.get("outbox.attempts").tag("outcome", "stale").counter().count()).isEqualTo(2);
    }

    @Test void bookkeepingFailureStopsPassWithoutReschedulingAcknowledgedSend() {
        oneClaim();
        when(store.markDelivered(any(), any())).thenThrow(new IllegalStateException("db down"));
        assertThat(relay(m -> {}, m -> {}, 20).runOnce()).isEqualTo(1);
        verify(store, times(1)).claimNext(any());
        verify(store, never()).reschedule(any(), any(), any(), any());
    }

    @Test void rescheduleAndClaimFailuresStopPassAndReleaseGuard() {
        oneClaim();
        when(store.reschedule(any(), any(), any(), any())).thenThrow(new IllegalStateException("db down"));
        var relay = relay(m -> { throw new OutboxPublishException("SEND_FAILED", null); }, m -> {}, 20);
        assertThat(relay.runOnce()).isEqualTo(1);
        verify(store, times(1)).claimNext(any());
        when(store.claimNext(any())).thenThrow(new IllegalStateException("db down"));
        assertThat(relay.runOnce()).isZero();
        doReturn(Optional.empty()).when(store).claimNext(any());
        assertThat(relay.runOnce()).isZero();
        verify(store, times(3)).claimNext(any());
    }

    @Test void concurrentPassCannotClaimWhilePublisherBlocked() throws Exception {
        oneClaim();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var relay = relay(m -> { entered.countDown(); assertThat(release.await(5, TimeUnit.SECONDS)).isTrue(); }, m -> {}, 1);
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            var pass = executor.submit(relay::runOnce);
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(relay.runOnce()).isZero();
            verify(store, times(1)).claimNext(any());
            release.countDown();
            assertThat(pass.get(5, TimeUnit.SECONDS)).isEqualTo(1);
            assertThat(relay.runOnce()).isZero();
        } finally { release.countDown(); executor.shutdownNow(); }
    }

    @Test void ambientTransactionRejectedBeforeClaim() {
        TransactionSynchronizationManager.setActualTransactionActive(true);
        assertThatThrownBy(() -> relay(m -> {}, m -> {}, 20).runOnce()).isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(store);
    }

    @Test void logsIdentifiersOutcomeLatencyAndCodeButNeverPayloadOrExceptionText() {
        oneClaim();
        Logger logger = (Logger) LoggerFactory.getLogger(OutboxRelay.class);
        var appender = new ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        try {
            relay(m -> { throw new OutboxPublishException("SEND_FAILED", new RuntimeException("DO_NOT_LOG")); }, m -> {}, 1).runOnce();
            String logs = appender.list.stream().map(e -> e.getFormattedMessage()).collect(java.util.stream.Collectors.joining());
            assertThat(logs).contains(claim.message().eventId().toString(), claim.message().orderId().toString(),
                    "test-correlation", "attempt=3", "latencyMs=", "SEND_FAILED", "outcome=").doesNotContain("DO_NOT_LOG");
            assertThat(appender.list).allSatisfy(e -> assertThat(e.getThrowableProxy()).isNull());
        } finally { logger.detachAppender(appender); appender.stop(); }
    }

    @SuppressWarnings("unchecked")
    @Test void publisherTimeoutNeverInstallsDeliveryCallbackAndLateCompletionDoesNotMark() throws Exception {
        oneClaim();
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, String>> future = spy(new CompletableFuture<>());
        doThrow(new TimeoutException()).when(future).get(12000, TimeUnit.MILLISECONDS);
        when(template.send(anyString(), anyString(), anyString())).thenReturn(future);
        when(store.reschedule(any(), any(), any(), any())).thenReturn(true);
        var publisher = new KafkaOutboxPublisher(template, OutboxProperties.defaults());
        assertThat(relay(publisher, m -> {}, 1).runOnce()).isEqualTo(1);
        future.complete(null);
        verify(store).reschedule(eq(claim.message().eventId()), eq(claim.token()), any(), eq("ACK_UNCERTAIN"));
        verify(store, never()).markDelivered(any(), any());
        verify(future).get(12000, TimeUnit.MILLISECONDS);
        verify(template).send(claim.message().topic(), claim.message().messageKey(), claim.message().payload());
    }

    @SuppressWarnings("unchecked")
    @Test void publisherMapsNestedAndSynchronousFailuresAndPreservesCauses() {
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        var publisher = new KafkaOutboxPublisher(template, OutboxProperties.defaults());
        var large = new RecordTooLargeException("large");
        when(template.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.failedFuture(large));
        assertThatThrownBy(() -> publisher.publish(claim.message())).isInstanceOfSatisfying(OutboxPublishException.class,
                ex -> assertThat(ex.code()).isEqualTo("RECORD_TOO_LARGE")).hasRootCause(large);
        var timeout = new org.apache.kafka.common.errors.TimeoutException("timeout");
        when(template.send(anyString(), anyString(), anyString())).thenThrow(timeout);
        assertThatThrownBy(() -> publisher.publish(claim.message())).isInstanceOfSatisfying(OutboxPublishException.class,
                ex -> assertThat(ex.code()).isEqualTo("ACK_UNCERTAIN")).hasCause(timeout);
        doThrow(new IllegalStateException("send")).when(template).send(anyString(), anyString(), anyString());
        assertThatThrownBy(() -> publisher.publish(claim.message())).isInstanceOfSatisfying(OutboxPublishException.class,
                ex -> assertThat(ex.code()).isEqualTo("SEND_FAILED"));
    }
}
