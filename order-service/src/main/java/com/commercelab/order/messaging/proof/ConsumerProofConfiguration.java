package com.commercelab.order.messaging.proof;

import com.commercelab.order.messaging.ConsumerCommitHook;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Configuration(proxyBeanMethods = false)
@Profile("consumer-proof")
@ConditionalOnProperty(name = "consumer.proof.enabled", havingValue = "true")
public class ConsumerProofConfiguration {
    @Bean
    @ConditionalOnMissingBean(name = "consumerProofPause")
    CountDownLatch consumerProofPause() { return new CountDownLatch(1); }

    @Bean
    @Primary
    ConsumerCommitHook consumerProofHook(
            @Value("${consumer.proof.event-id:}") String eventId,
            @Qualifier("consumerProofPause") CountDownLatch pause) {
        UUID selected;
        try {
            selected = UUID.fromString(eventId);
            if (!selected.toString().equals(eventId)) throw new IllegalArgumentException();
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("consumer.proof.event-id must be a canonical UUID");
        }
        return (id, outcome) -> {
            if (!selected.equals(id)) return;
            if (TransactionSynchronizationManager.isActualTransactionActive()) {
                throw new IllegalStateException("Consumer proof boundary must be outside a database transaction");
            }
            // Flushed process output synchronizes the external SIGKILL harness; no network control surface.
            System.out.println("DB_COMMIT_BOUNDARY eventId=" + selected + " outcome=" + outcome);
            System.out.flush();
            try {
                pause.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Consumer proof pause interrupted", interrupted);
            }
        };
    }
}
