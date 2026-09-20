package com.commercelab.inventory.outbox;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.commercelab.inventory.messaging.InventoryEventHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

// Enabled transport wiring, but a long initial delay keeps manually faulted passes deterministic.
@SpringBootTest(properties = {"inventory.outbox.enabled=true", "inventory.outbox.poll-interval-ms=600000",
        "spring.kafka.producer.properties.max.request.size=1024"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Timeout(120)
class KafkaInventoryOutboxIT {
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
    // Keep the advertised endpoint stable across Docker stop/start of this broker.
    static final int BROKER_PORT = org.springframework.test.util.TestSocketUtils.findAvailableTcpPort();
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.7.1")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    new com.github.dockerjava.api.model.PortBinding(
                            com.github.dockerjava.api.model.Ports.Binding.bindPort(BROKER_PORT),
                            new com.github.dockerjava.api.model.ExposedPort(9092))));
    static {
        POSTGRES.start();
        try {
            KAFKA.start();
            try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic("commerce.inventory.v1", 3, (short) 1)
                        .configs(Map.of("cleanup.policy", "delete", "retention.ms", "604800000"))))
                        .all().get(30, TimeUnit.SECONDS);
            }
        } catch (Exception ex) {
            try { KAFKA.stop(); } finally { POSTGRES.stop(); }
            throw new ExceptionInInitializerError(ex);
        }
    }

    @DynamicPropertySource static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired InventoryEventHandler handler;
    @Autowired OutboxRelay relay;
    @Autowired OutboxDeliveryStore store;
    @Autowired OutboxPublisher publisher;
    @Autowired OutboxMetrics metrics;
    @Autowired OutboxProperties properties;
    @Autowired ObjectMapper mapper;
    @Autowired MeterRegistry meters;

    @BeforeEach void clear() {
        jdbc.execute("TRUNCATE inventory_event_inbox, inventory_reservation_attempts, inventory_reservations, stock CASCADE");
    }

    @AfterAll static void stop(@Autowired ThreadPoolTaskScheduler scheduler,
            @Autowired ProducerFactory<String, String> factory) throws InterruptedException {
        try {
            try {
                scheduler.shutdown();
                assertThat(scheduler.getScheduledThreadPoolExecutor().awaitTermination(20, TimeUnit.SECONDS)).isTrue();
            } finally { factory.reset(); }
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(Thread.getAllStackTraces().keySet()).noneMatch(t ->
                            t.isAlive() && t.getName().startsWith("kafka-producer-network-thread")));
        } finally {
            try { KAFKA.stop(); } finally { POSTGRES.stop(); }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void publishesBothStoredOutcomesWithoutRegeneration(boolean rejected) throws Exception {
        var message = seed(rejected, false);
        var before = immutableRow(message);
        assertThat(mapper.readTree(message.payload()).path("eventType").asText())
                .isEqualTo(rejected ? "InventoryRejected" : "InventoryReserved");
        var checkedRelay = relay(m -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            assertThat(m).isEqualTo(message);
            publisher.publish(m);
        }, m -> assertThat(row(m).get("delivered_at")).isNull());
        assertThat(checkedRelay.runOnce()).isEqualTo(1);
        assertThat(row(message).get("delivered_at")).isNotNull();
        assertThat(immutableRow(message)).isEqualTo(before);
        assertRecords(message, 1);
    }

    @Test void unavailableBrokerDefersThenRecoversTheExactStoredResult() {
        String containerId = KAFKA.getContainerId();
        KAFKA.getDockerClient().stopContainerCmd(containerId).withTimeout(10).exec();
        OutboxMessage message;
        Map<String, Object> before;
        try {
            long started = System.nanoTime();
            message = seed(false, false);
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(5));
            before = immutableRow(message);
            started = System.nanoTime();
            assertThat(relay.runOnce()).isEqualTo(1);
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(16));
            assertThat(row(message)).containsEntry("delivered_at", null).containsEntry("lease_token", null)
                    .containsEntry("last_error_code", "ACK_UNCERTAIN");
            assertThat(immutableRow(message)).isEqualTo(before);
        } finally { KAFKA.getDockerClient().startContainerCmd(containerId).exec(); }
        assertThat(KAFKA.getContainerId()).isEqualTo(containerId);
        due(message);
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(250)).untilAsserted(() -> {
            relay.runOnce();
            assertThat(row(message).get("delivered_at")).isNotNull();
        });
        assertThat(row(message).get("attempt_count"))
                .isInstanceOfSatisfying(Long.class, attempts -> assertThat(attempts).isGreaterThan(1));
        assertThat(immutableRow(message)).isEqualTo(before);
        assertRecords(message, 1);
    }

    @Test void acknowledgedButUnmarkedSendIsRepeatedWithSameIdentityAndBytes() {
        var message = seed(true, false);
        var before = immutableRow(message);
        assertThat(relay(publisher, m -> { throw new IllegalStateException("injected post-ack failure"); })
                .runOnce()).isEqualTo(1);
        assertThat(row(message).get("delivered_at")).isNull();
        assertThat(row(message).get("lease_token")).isNotNull();
        jdbc.update("UPDATE inventory_result_outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=?",
                message.eventId());
        assertThat(relay.runOnce()).isEqualTo(1);
        assertThat(row(message).get("delivered_at")).isNotNull();
        assertThat(row(message)).containsEntry("attempt_count", 2L);
        assertThat(immutableRow(message)).isEqualTo(before);
        assertRecords(message, 2);
    }

    @Test void oversizedRecordDoesNotStarveLaterDueWorkOrLeakPayload() {
        var large = seed(true, true);
        var healthy = seed(false, false);
        var before = immutableRow(large);
        jdbc.update("UPDATE inventory_result_outbox SET next_attempt_at=clock_timestamp()-interval '1 hour' WHERE event_id=?",
                large.eventId());
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        var logs = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        logs.start();
        logger.addAppender(logs);
        try {
            assertThat(relay.runOnce()).isEqualTo(2);
            assertThat(logs.list).allSatisfy(event -> {
                assertThat(event.getFormattedMessage()).doesNotContain(large.payload());
                assertThat(event.getLoggerName()).isNotEqualTo("org.springframework.kafka.support.LoggingProducerListener");
            });
        } finally { logger.detachAppender(logs); logs.stop(); }
        assertThat(row(large)).containsEntry("delivered_at", null).containsEntry("last_error_code", "RECORD_TOO_LARGE")
                .containsEntry("lease_token", null);
        assertThat(immutableRow(large)).isEqualTo(before);
        assertThat(row(healthy).get("delivered_at")).isNotNull();
        assertRecords(healthy, 1);
        metrics.refresh();
        assertThat(meters.get("outbox.pending").gauge().value()).isEqualTo(1);
        assertThat(meters.get("outbox.failed.pending").gauge().value()).isEqualTo(1);
    }

    private OutboxRelay relay(OutboxPublisher transport, OutboxPublicationHook hook) {
        return new OutboxRelay(store, new OutboxRetryPolicy(() -> 0), transport, hook, properties, metrics, mapper);
    }

    private OutboxMessage seed(boolean rejected, boolean large) {
        return InventoryOutboxTestSupport.seed(handler, jdbc, rejected, large);
    }

    private Map<String, Object> row(OutboxMessage message) {
        return jdbc.queryForMap("SELECT * FROM inventory_result_outbox WHERE event_id=?", message.eventId());
    }

    private Map<String, Object> immutableRow(OutboxMessage message) {
        return jdbc.queryForMap("SELECT event_id, order_id, causation_id, event_type, schema_version, "
                + "topic, message_key, payload, created_at FROM inventory_result_outbox WHERE event_id=?", message.eventId());
    }

    private void due(OutboxMessage message) {
        jdbc.update("UPDATE inventory_result_outbox SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE event_id=?",
                message.eventId());
    }

    private void assertRecords(OutboxMessage message, int expected) {
        try (var consumer = new KafkaConsumer<>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "group.id", "inventory-relay-it-" + UUID.randomUUID(), "auto.offset.reset", "earliest",
                "enable.auto.commit", false, "default.api.timeout.ms", 15000),
                new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("commerce.inventory.v1"));
            int found = 0;
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    if (message.messageKey().equals(record.key())) {
                        assertThat(record.topic()).isEqualTo(message.topic());
                        assertThat(record.value()).isEqualTo(message.payload());
                        if (++found == expected) return;
                    }
                }
            }
            fail("Expected " + expected + " records for event " + message.eventId() + ", found " + found);
        }
    }
}
