package com.commercelab.inventory.outbox;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.springframework.kafka.core.KafkaTemplate;

public class KafkaOutboxPublisher implements OutboxPublisher {
    private final KafkaTemplate<String, String> template;
    private final OutboxProperties properties;

    public KafkaOutboxPublisher(KafkaTemplate<String, String> template, OutboxProperties properties) {
        this.template = template;
        this.properties = properties;
    }

    @Override
    public void publish(OutboxMessage message) throws InterruptedException {
        try {
            template.send(message.topic(), message.messageKey(), message.payload())
                    .get(properties.ackWaitMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            throw ex;
        } catch (ExecutionException | TimeoutException | RuntimeException ex) {
            for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
                if (cause instanceof InterruptedException || cause instanceof InterruptException) {
                    var interrupted = new InterruptedException("Kafka publication interrupted");
                    interrupted.initCause(ex);
                    throw interrupted;
                }
                if (cause instanceof RecordTooLargeException) {
                    throw new OutboxPublishException("RECORD_TOO_LARGE", ex);
                }
                if (cause instanceof TimeoutException || cause instanceof org.apache.kafka.common.errors.TimeoutException) {
                    throw new OutboxPublishException("ACK_UNCERTAIN", ex);
                }
            }
            throw new OutboxPublishException("SEND_FAILED", ex);
        }
    }
}
