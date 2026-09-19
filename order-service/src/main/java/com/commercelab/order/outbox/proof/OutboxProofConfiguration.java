package com.commercelab.order.outbox.proof;

import com.commercelab.order.outbox.OutboxPublicationHook;
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

@Configuration(proxyBeanMethods = false)
@Profile("outbox-proof")
@ConditionalOnProperty(name = "order.outbox.proof.enabled", havingValue = "true")
public class OutboxProofConfiguration {
    @Bean
    @ConditionalOnMissingBean(name = "outboxProofPause")
    CountDownLatch outboxProofPause() { return new CountDownLatch(1); }

    @Bean
    @Primary
    OutboxPublicationHook outboxProofHook(
            @Value("${order.outbox.proof.event-id:}") String eventId,
            @Qualifier("outboxProofPause") CountDownLatch pause) {
        UUID selected;
        try {
            selected = UUID.fromString(eventId);
            if (!selected.toString().equals(eventId)) throw new IllegalArgumentException();
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("order.outbox.proof.event-id must be a canonical UUID");
        }
        return message -> {
            if (!selected.equals(message.eventId())) return;
            // Direct, flushed JSON is the harness synchronization boundary, not a control endpoint.
            System.out.println("{\"marker\":\"ACK_BOUNDARY\",\"eventId\":\"" + selected
                    + "\",\"orderId\":\"" + message.orderId() + "\"}");
            System.out.flush();
            pause.await();
        };
    }
}
