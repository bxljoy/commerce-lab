package com.commercelab.order.outbox;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.commercelab.order.AbstractPostgresIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(properties = {"order.outbox.enabled=false", "spring.kafka.producer.properties.max.request.size=1024"})
@AutoConfigureMockMvc
@Import(KafkaOutboxIT.HookConfiguration.class)
class KafkaOutboxIT extends AbstractPostgresIntegrationTest {
    // Docker can reassign a randomly bound host port on start. Keep the advertised endpoint stable.
    static final int BROKER_PORT = org.springframework.test.util.TestSocketUtils.findAvailableTcpPort();
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.7.1")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    new com.github.dockerjava.api.model.PortBinding(
                            com.github.dockerjava.api.model.Ports.Binding.bindPort(BROKER_PORT),
                            new com.github.dockerjava.api.model.ExposedPort(9092))));
    static final AtomicInteger HOOK_CALLS = new AtomicInteger();
    static {
        KAFKA.start();
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("commerce.orders.v1", 3, (short) 1)
                    .configs(Map.of("cleanup.policy", "delete", "retention.ms", "604800000"))))
                    .all().get(30, TimeUnit.SECONDS);
        } catch (Exception ex) { throw new ExceptionInInitializerError(ex); }
    }

    @DynamicPropertySource static void kafka(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class HookConfiguration {
        @Bean
        @org.springframework.context.annotation.Primary
        OutboxPublicationHook checkingHook() {
            return message -> {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                HOOK_CALLS.incrementAndGet();
            };
        }
    }

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;
    @Autowired OutboxRelay relay;
    @Autowired OutboxDeliveryStore store;
    @Autowired MeterRegistry meters;
    @SpyBean KafkaTemplate<String, String> template;

    @BeforeEach void clear() {
        jdbc.execute("TRUNCATE order_outbox, order_requests, order_lines, orders CASCADE");
        HOOK_CALLS.set(0);
        doAnswer(invocation -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            return invocation.callRealMethod();
        }).when(template).send(anyString(), anyString(), anyString());
    }
    @AfterAll static void stop() { KAFKA.stop(); }

    @Test void acknowledgedRecordMatchesStoredValueAndKeyAndOrderStaysPending() throws Exception {
        UUID order = createOrder(false);
        var row = row(order);
        assertThat(row.get("attempt_count")).isEqualTo(0L);
        assertThat(relay.runOnce()).isEqualTo(1);
        assertRecord(row);
        assertThat(row(order).get("delivered_at")).isNotNull();
        assertPending(order);
        assertThat(HOOK_CALLS.get()).isEqualTo(1);
        assertThat(store.stats().pendingCount()).isZero();
    }

    @Test void stoppedBrokerDoesNotBlockHttpAcceptanceAndSameStorageRecoversRealRecord() throws Exception {
        String containerId = KAFKA.getContainerId();
        KAFKA.getDockerClient().stopContainerCmd(containerId).withTimeout(10).exec();
        UUID order;
        Map<String, Object> stored;
        try {
            long start = System.nanoTime();
            order = createOrder(false);
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
            stored = row(order);
            start = System.nanoTime();
            assertThat(relay.runOnce()).isEqualTo(1);
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(16));
            var failed = row(order);
            assertThat(failed.get("delivered_at")).isNull();
            assertThat(failed.get("last_error_code")).isEqualTo("ACK_UNCERTAIN");
            assertThat(failed.get("lease_token")).isNull();
            assertThat(store.stats().failedPendingCount()).isEqualTo(1);
            assertThat(HOOK_CALLS.get()).isZero();
            assertPending(order);
        } finally {
            // Docker start preserves this exact container's writable storage and mapped ports.
            KAFKA.getDockerClient().startContainerCmd(containerId).exec();
        }
        assertThat(KAFKA.getContainerId()).isEqualTo(containerId);
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (row(order).get("delivered_at") == null && System.nanoTime() < deadline) {
            relay.runOnce();
            Thread.sleep(250);
        }
        assertThat(row(order).get("delivered_at")).isNotNull();
        assertThat(row(order).get("attempt_count")).isInstanceOfSatisfying(Long.class, n -> assertThat(n).isGreaterThan(1));
        assertRecord(stored);
        assertPending(order);
    }

    @Test void oversizedMessageRemainsVisibleWithoutStarvingLaterHealthyEvent() throws Exception {
        UUID oversized = createOrder(true);
        UUID healthy = createOrder(false);
        var healthyRow = row(healthy);
        assertThat(relay.runOnce()).isEqualTo(2);
        var failed = row(oversized);
        assertThat(failed.get("delivered_at")).isNull();
        assertThat(failed.get("last_error_code")).isEqualTo("RECORD_TOO_LARGE");
        assertThat(failed.get("lease_token")).isNull();
        assertThat(row(healthy).get("delivered_at")).isNotNull();
        assertRecord(healthyRow);
        assertThat(store.stats().pendingCount()).isEqualTo(1);
        assertThat(meters.get("outbox.failed.pending").gauge().value()).isEqualTo(1);
        assertThat(meters.get("outbox.pending").gauge().value()).isEqualTo(1);
        assertThat(meters.get("outbox.oldest.pending.age").gauge().value()).isGreaterThan(0);
        assertPending(oversized);
        assertPending(healthy);
        assertThat(HOOK_CALLS.get()).isEqualTo(1);
    }

    UUID createOrder(boolean large) throws Exception {
        var lines = new java.util.ArrayList<Map<String, Object>>();
        for (int i = 0; i < (large ? 40 : 1); i++) {
            lines.add(Map.of("sku", "SKU-" + i + (large ? "x".repeat(50) : ""), "quantity", 1, "unitPrice", 1));
        }
        var response = mvc.perform(post("/api/v1/orders")
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .header("X-Correlation-ID", "kafka-it").contentType("application/json")
                .content(mapper.writeValueAsString(Map.of("customerId", "customer", "currency", "EUR", "lines", lines))))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.status").value("PENDING_INVENTORY"))
                .andReturn().getResponse();
        return UUID.fromString(mapper.readTree(response.getContentAsString()).path("id").textValue());
    }

    Map<String, Object> row(UUID order) { return jdbc.queryForMap("SELECT * FROM order_outbox WHERE order_id=?", order); }
    void assertPending(UUID order) {
        assertThat(jdbc.queryForObject("SELECT status FROM orders WHERE id=?", String.class, order)).isEqualTo("PENDING_INVENTORY");
    }

    void assertRecord(Map<String, Object> row) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        props.put("group.id", "outbox-it-" + UUID.randomUUID());
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        props.put("default.api.timeout.ms", "15000");
        try (var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("commerce.orders.v1"));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.key().equals(row.get("message_key"))) {
                        assertThat(record.value()).isEqualTo(row.get("payload"));
                        assertThat(record.topic()).isEqualTo(row.get("topic"));
                        return;
                    }
                }
            }
            fail("No Kafka record for event " + row.get("event_id"));
        }
    }
}
