package com.commercelab.order.outbox;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.commercelab.order.outbox.proof.OutboxProofConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith(OutputCaptureExtension.class)
class OutboxProofConfigurationTest {
    final UUID selected = UUID.randomUUID();
    final CountDownLatch pause = new CountDownLatch(1);
    final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(OutboxConfiguration.class, OutboxProofConfiguration.class)
            .withPropertyValues("order.outbox.enabled=false")
            .withBean(OutboxDeliveryStore.class, () -> mock(OutboxDeliveryStore.class))
            .withBean(OutboxRetryPolicy.class, OutboxRetryPolicy::new)
            .withBean(ObjectMapper.class, ObjectMapper::new)
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
            .withBean("outboxProofPause", CountDownLatch.class, () -> pause);

    @ParameterizedTest
    @ValueSource(strings = {"", "spring.profiles.active=outbox-proof", "order.outbox.proof.enabled=true",
            "spring.profiles.active=production"})
    void absentEitherGuardLeavesNormalNoOp(String property, CapturedOutput output) {
        runner.withPropertyValues(property).run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(OutboxPublicationHook.class);
            assertThat(context).doesNotHaveBean("outboxProofHook");
            assertThatCode(() -> context.getBean(OutboxPublicationHook.class)
                    .afterAcknowledgement(message(selected))).doesNotThrowAnyException();
            assertThat(output).doesNotContain("ACK_BOUNDARY");
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "invalid", "1-1-1-1-1"})
    void invalidSelectedEventFailsExplicitly(String value) {
        guarded().withPropertyValues("order.outbox.proof.event-id=" + value).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseMessage(
                    "order.outbox.proof.event-id must be a canonical UUID");
        });
    }

    @Test void selectedHookIsPrimaryAndOnlySelectedEventPausesInterruptibly(CapturedOutput output) {
        guarded().withPropertyValues("order.outbox.proof.event-id=" + selected).run(context -> {
            assertThat(context).hasNotFailed();
            var hook = context.getBean(OutboxPublicationHook.class);
            assertThat(hook).isSameAs(context.getBean("outboxProofHook"));
            hook.afterAcknowledgement(message(UUID.randomUUID()));
            assertThat(output).doesNotContain("ACK_BOUNDARY");
            var interrupted = new AtomicBoolean();
            Thread worker = new Thread(() -> {
                try { hook.afterAcknowledgement(message(selected)); }
                catch (InterruptedException expected) { interrupted.set(true); }
            });
            worker.start();
            try {
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                    assertThat(output).contains("\"marker\":\"ACK_BOUNDARY\"", "\"eventId\":\"" + selected + "\"");
                    assertThat(worker.getState()).isEqualTo(Thread.State.WAITING);
                });
                assertThat(pause.getCount()).isEqualTo(1);
                worker.interrupt();
                worker.join(3000);
                assertThat(worker.isAlive()).isFalse();
                assertThat(interrupted).isTrue();
            } finally { worker.interrupt(); worker.join(3000); }
        });
    }

    @Test void injectedLatchCanReleaseSelectedEvent() {
        guarded().withPropertyValues("order.outbox.proof.event-id=" + selected).run(context -> {
            pause.countDown();
            assertThatCode(() -> context.getBean(OutboxPublicationHook.class)
                    .afterAcknowledgement(message(selected))).doesNotThrowAnyException();
        });
    }

    ApplicationContextRunner guarded() {
        return runner.withPropertyValues("spring.profiles.active=outbox-proof", "order.outbox.proof.enabled=true");
    }

    OutboxMessage message(UUID eventId) {
        UUID orderId = UUID.randomUUID();
        return new OutboxMessage(eventId, orderId, "commerce.orders.v1", orderId.toString(), "{}");
    }
}
