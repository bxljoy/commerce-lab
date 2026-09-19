package com.commercelab.order.outbox;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.commercelab.order.service.OrderCreationService;
import com.commercelab.order.service.PlaceOrderCommand;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(properties = {"order.outbox.enabled=true", "order.outbox.poll-interval-ms=100",
        "spring.kafka.producer.properties.max.request.size=1024"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class KafkaOutboxScheduledIT {
    // Isolated DB: the enabled scheduler must not claim rows owned by another IT.
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.7.1");
    static {
        POSTGRES.start();
        KAFKA.start();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("commerce.orders.v1", 3, (short) 1)
                    .configs(Map.of("cleanup.policy", "delete", "retention.ms", "604800000"))))
                    .all().get(30, TimeUnit.SECONDS);
        } catch (Exception ex) { throw new ExceptionInInitializerError(ex); }
    }

    @DynamicPropertySource static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired OrderCreationService creation;
    @Autowired JdbcTemplate jdbc;
    @Autowired MeterRegistry meters;

    @AfterAll static void stop(@Autowired ThreadPoolTaskScheduler scheduler,
            @Autowired ProducerFactory<String, String> factory) throws InterruptedException {
        // Stop scheduled work and its producer before stopping either owned dependency.
        try {
            scheduler.shutdown();
            assertThat(scheduler.getScheduledThreadPoolExecutor().awaitTermination(20, TimeUnit.SECONDS)).isTrue();
            factory.reset();
        }
        finally {
            try { KAFKA.stop(); }
            finally { POSTGRES.stop(); }
        }
    }

    @Test void scheduledPassPublishesAndScheduledSnapshotExposesRetainedFailure() {
        UUID oversized = create(true);
        UUID healthy = create(false);
        String payload = jdbc.queryForObject("SELECT payload FROM order_outbox WHERE order_id=?", String.class, healthy);

        // Neither runOnce nor metrics.refresh is called by this test.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(jdbc.queryForObject("SELECT delivered_at IS NOT NULL FROM order_outbox WHERE order_id=?",
                    Boolean.class, healthy)).isTrue();
            assertThat(jdbc.queryForObject("SELECT last_error_code FROM order_outbox WHERE order_id=?",
                    String.class, oversized)).isEqualTo("RECORD_TOO_LARGE");
            for (String outcome : List.of("delivered", "retry")) {
                assertThat(meters.find("outbox.attempts").tag("outcome", outcome).counter()).isNotNull();
                assertThat(meters.find("outbox.publication").tag("outcome", outcome).timer()).isNotNull();
            }
            assertThat(meters.get("outbox.attempts").tag("outcome", "delivered").counter().count()).isEqualTo(1);
            assertThat(meters.get("outbox.publication").tag("outcome", "delivered").timer().count()).isEqualTo(1);
            assertThat(meters.get("outbox.attempts").tag("outcome", "retry").counter().count()).isGreaterThanOrEqualTo(1);
            assertThat(meters.get("outbox.publication").tag("outcome", "retry").timer().count()).isGreaterThanOrEqualTo(1);
            assertThat(meters.get("outbox.pending").gauge().value()).isEqualTo(1);
            assertThat(meters.get("outbox.failed.pending").gauge().value()).isEqualTo(1);
            assertThat(meters.get("outbox.oldest.pending.age").gauge().value()).isGreaterThan(0);
            assertThat(meters.get("outbox.snapshot.stale").gauge().value()).isZero();
            assertThat(meters.get("outbox.snapshot.age").gauge().value()).isBetween(0.0, 5.0);
        });
        assertThat(jdbc.queryForObject("SELECT count(*) FROM orders WHERE status='PENDING_INVENTORY'", Long.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT delivered_at IS NULL FROM order_outbox WHERE order_id=?", Boolean.class, oversized)).isTrue();

        try (var consumer = new KafkaConsumer<>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "group.id", "scheduled-it-" + UUID.randomUUID(), "auto.offset.reset", "earliest",
                "enable.auto.commit", false, "default.api.timeout.ms", 15000),
                new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("commerce.orders.v1"));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    if (healthy.toString().equals(record.key())) {
                        assertThat(record.value()).isEqualTo(payload);
                        return;
                    }
                }
            }
            fail("No scheduled Kafka record for order " + healthy);
        }
    }

    private UUID create(boolean large) {
        var lines = IntStream.range(0, large ? 40 : 1)
                .mapToObj(i -> new PlaceOrderCommand.Line("SKU-" + i + (large ? "x".repeat(50) : ""), 1, BigDecimal.ONE))
                .toList();
        return creation.createOrReplay(UUID.randomUUID().toString(),
                new PlaceOrderCommand("customer", "EUR", lines), "scheduled-it").order().id();
    }
}
