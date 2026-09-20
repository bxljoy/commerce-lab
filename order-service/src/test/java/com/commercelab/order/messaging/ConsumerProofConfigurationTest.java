package com.commercelab.order.messaging;

import static org.assertj.core.api.Assertions.*;

import com.commercelab.order.messaging.proof.ConsumerProofConfiguration;
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
import org.springframework.transaction.support.TransactionSynchronizationManager;

@ExtendWith(OutputCaptureExtension.class)
class ConsumerProofConfigurationTest {
    final UUID selected = UUID.randomUUID();
    final CountDownLatch pause = new CountDownLatch(1);
    final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(ConsumerProofConfiguration.class)
            .withBean("normalHook", ConsumerCommitHook.class, () -> (id, outcome) -> {})
            .withBean("consumerProofPause", CountDownLatch.class, () -> pause);

    @ParameterizedTest
    @ValueSource(strings = {"", "spring.profiles.active=consumer-proof",
            "consumer.proof.enabled=true", "spring.profiles.active=production",
            "consumer.proof.enabled=false"})
    void bothGuardsRequired(String property, CapturedOutput output) {
        runner.withPropertyValues(property).run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(ConsumerCommitHook.class);
            assertThat(context).doesNotHaveBean("consumerProofHook");
            context.getBean(ConsumerCommitHook.class).afterDatabaseCommit(selected, ProcessingOutcome.APPLIED);
            assertThat(output).doesNotContain("DB_COMMIT_BOUNDARY");
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "invalid", "1-1-1-1-1", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"})
    void canonicalIdentityRequired(String id) {
        guarded().withPropertyValues("consumer.proof.event-id=" + id).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseMessage(
                    "consumer.proof.event-id must be a canonical UUID");
        });
    }

    @Test void primarySelectedHookWaitsInterruptiblyOutsideTransaction(CapturedOutput output) {
        guarded().withPropertyValues("consumer.proof.event-id=" + selected).run(context -> {
            var hook = context.getBean(ConsumerCommitHook.class);
            assertThat(hook).isSameAs(context.getBean("consumerProofHook"));
            hook.afterDatabaseCommit(UUID.randomUUID(), ProcessingOutcome.APPLIED);
            assertThat(output).doesNotContain("DB_COMMIT_BOUNDARY");
            var interrupted = new AtomicBoolean();
            var worker = new Thread(() -> {
                try { hook.afterDatabaseCommit(selected, ProcessingOutcome.APPLIED); }
                catch (IllegalStateException expected) {
                    interrupted.set(Thread.currentThread().isInterrupted());
                }
            });
            worker.start();
            try {
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                    assertThat(output).contains("DB_COMMIT_BOUNDARY eventId=" + selected + " outcome=APPLIED");
                    assertThat(worker.getState()).isEqualTo(Thread.State.WAITING);
                });
                worker.interrupt();
                worker.join(3000);
                assertThat(worker.isAlive()).isFalse();
                assertThat(interrupted).isTrue();
            } finally { worker.interrupt(); worker.join(3000); }
        });
    }

    @Test void refusesToPauseInsideDatabaseTransaction(CapturedOutput output) {
        guarded().withPropertyValues("consumer.proof.event-id=" + selected).run(context -> {
            TransactionSynchronizationManager.setActualTransactionActive(true);
            try {
                assertThatThrownBy(() -> context.getBean(ConsumerCommitHook.class)
                        .afterDatabaseCommit(selected, ProcessingOutcome.APPLIED))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("Consumer proof boundary must be outside a database transaction");
                assertThat(output).doesNotContain("DB_COMMIT_BOUNDARY");
            } finally { TransactionSynchronizationManager.setActualTransactionActive(false); }
        });
    }

    ApplicationContextRunner guarded() {
        return runner.withPropertyValues("spring.profiles.active=consumer-proof", "consumer.proof.enabled=true");
    }
}
