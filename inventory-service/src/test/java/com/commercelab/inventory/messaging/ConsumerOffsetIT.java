package com.commercelab.inventory.messaging;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.MeterRegistry;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(properties = {"inventory.outbox.enabled=false", "inventory.events.enabled=false"})
@Import(ConsumerOffsetIT.Hooks.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Timeout(150)
class ConsumerOffsetIT {
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
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
                admin.createTopics(List.of(new NewTopic(ConsumerConfiguration.TOPIC, 3, (short) 1)))
                        .all().get(30, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            try { KAFKA.stop(); } finally { POSTGRES.stop(); }
            throw new ExceptionInInitializerError(e);
        }
    }
    @DynamicPropertySource static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired OrderPlacedListener listener;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired MeterRegistry meters;
    @Autowired Gate hook;
    @Autowired ConcurrentKafkaListenerContainerFactory<String, String> workflowKafkaListenerContainerFactory;
    @Autowired org.springframework.kafka.core.ConsumerFactory<String, String> workflowConsumerFactory;
    @Autowired PartitionFailureHandler failures;
    @Autowired org.springframework.kafka.config.KafkaListenerEndpointRegistry listeners;
    AdminClient admin;
    KafkaProducer<String, byte[]> producer;
    ConcurrentMessageListenerContainer<String, String> container;
    final List<ConcurrentMessageListenerContainer<String, String>> containers = new ArrayList<>();
    String group;
    final TopicPartition p0 = new TopicPartition(ConsumerConfiguration.TOPIC, 0);
    final AtomicBoolean failCommit = new AtomicBoolean();
    final AtomicInteger commitFailures = new AtomicInteger();

    @BeforeEach void setup() throws Exception {
        var registered = listeners.getListenerContainer("inventory-workflow");
        assertThat(registered).isNotNull();
        assertThat(registered.isAutoStartup()).isFalse();
        assertThat(registered.getContainerProperties().getGroupId()).isEqualTo(ConsumerConfiguration.GROUP);
        hook.action = (id, outcome) -> {};
        jdbc.execute("TRUNCATE inventory_event_inbox, inventory_reservation_attempts, inventory_reservations, stock CASCADE");
        admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "default.api.timeout.ms", 10000, "request.timeout.ms", 5000));
        producer = new KafkaProducer<>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "acks", "all", "max.block.ms", 10000, "delivery.timeout.ms", 15000, "request.timeout.ms", 5000),
                new StringSerializer(), new ByteArraySerializer());
        group = "task5-" + UUID.randomUUID();
        // Isolate test histories without changing the production group's offset policy.
        Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
        for (int p = 0; p < 3; p++) latest.put(new TopicPartition(p0.topic(), p), OffsetSpec.latest());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        admin.listOffsets(latest).all().get(15, TimeUnit.SECONDS)
                .forEach((tp, info) -> offsets.put(tp, new OffsetAndMetadata(info.offset())));
        admin.alterConsumerGroupOffsets(group, offsets).all().get(15, TimeUnit.SECONDS);
    }

    @AfterEach void cleanup() {
        hook.release.countDown();
        for (var c : containers) c.stop();
        containers.clear();
        if (producer != null) producer.close(Duration.ofSeconds(5));
        if (admin != null) admin.close(Duration.ofSeconds(5));
    }
    @AfterAll static void shutdown() {
        try { KAFKA.stop(); } finally { POSTGRES.stop(); }
    }

    @ParameterizedTest
    @CsvSource({"1,json", "3,json", "1,tombstone", "1,utf"})
    void poisonCannotSkipAcrossRestartRebalanceOrSessionTimeout(int concurrency, String kind) throws Exception {
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        var logs = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        logs.list = new CopyOnWriteArrayList<>();
        logs.start(); logger.addAppender(logs);
        try {
            container = start(concurrency, workflowKafkaListenerContainerFactory);
            byte[] poison = switch (kind) {
                case "tombstone" -> null;
                case "utf" -> new byte[] {(byte) 0xff, (byte) 0xc3};
                default -> "{sensitive-task5-secret".getBytes(StandardCharsets.UTF_8);
            };
            long bad = send(0, "private-task5-key", poison);
            var follower = event();
            send(0, follower);
            var healthy = event();
            long healthyOffset = send(1, healthy);
            applied(healthy);
            committed(1, healthyOffset + 1);
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(container.isPartitionPaused(p0)).isTrue());
            noSkip(bad, follower);

            // One consumer owns all partitions in the concurrency=1 case. Polling must stay active.
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(memberIds()).hasSize(concurrency));
            var members = memberIds();
            await().during(Duration.ofSeconds(11)).atMost(Duration.ofSeconds(16)).untilAsserted(() -> {
                assertThat(assigned()).isEqualTo(3);
                assertThat(memberIds()).isEqualTo(members);
                noSkip(bad, follower);
            });
            double blockedBefore = blockedCount();
            container.stop();
            container.start();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(blockedCount()).isGreaterThan(blockedBefore));
            noSkip(bad, follower);
            double beforeJoin = blockedCount();
            var other = start(1, workflowKafkaListenerContainerFactory);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(memberIds()).hasSize(concurrency + 1));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(blockedCount()).isGreaterThan(beforeJoin));
            var afterRebalance = event();
            long next = send(1, afterRebalance);
            applied(afterRebalance);
            committed(1, next + 1);
            noSkip(bad, follower);
            other.stop();
            var afterLeave = event();
            long last = send(1, afterLeave);
            applied(afterLeave);
            committed(1, last + 1);
            noSkip(bad, follower);
            assertThat(logs.list).allSatisfy(log -> {
                assertThat(log.getFormattedMessage()).doesNotContain("sensitive-task5-secret", "private-task5-key");
                if (log.getThrowableProxy() != null)
                    assertThat(ch.qos.logback.classic.spi.ThrowableProxyUtil.asString(log.getThrowableProxy()))
                            .doesNotContain("sensitive-task5-secret", "private-task5-key");
            });
        } finally { logger.detachAppender(logs); logs.stop(); }
    }

    @Test void blockedConsumerStopsPromptlyAndClearsChildPauseRequest() throws Exception {
        container = start(1, workflowKafkaListenerContainerFactory);
        long offset = send(0, "private-key", "{poison".getBytes(StandardCharsets.UTF_8));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(container.isPartitionPaused(p0)).isTrue());
        var child = container.getContainers().getFirst();
        long start = System.nanoTime();
        container.stop();
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(3));
        assertThat(child.isPartitionPauseRequested(p0)).isFalse();
        assertThat(committedOffset(0)).isLessThanOrEqualTo(offset);
    }

    @Test void databaseSerializationFailureRollsBackThenReplaysTheSameOffset() throws Exception {
        jdbc.execute("CREATE OR REPLACE FUNCTION task5_transient() RETURNS trigger LANGUAGE plpgsql AS $$ "
                + "BEGIN RAISE EXCEPTION 'temporary test failure' USING ERRCODE='40001'; RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER task5_transient BEFORE INSERT ON inventory_event_inbox "
                + "FOR EACH ROW EXECUTE FUNCTION task5_transient()");
        var event = event();
        double before = retryCount();
        long offset;
        try {
            container = start(1, workflowKafkaListenerContainerFactory);
            offset = send(0, event);
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(retryCount()).isGreaterThan(before);
                assertThat(inbox(event.id())).isZero();
                assertThat(committedOffset(0)).isLessThanOrEqualTo(offset);
            });
        } finally {
            jdbc.execute("DROP TRIGGER task5_transient ON inventory_event_inbox");
            jdbc.execute("DROP FUNCTION task5_transient()");
        }
        applied(event);
        committed(0, offset + 1);
        assertThat(inbox(event.id())).isOne();
    }

    @Test void hookSeesCommittedStateOutsideTransactionWhileOffsetIsStillUncommitted() throws Exception {
        var event = event();
        hook.release = new CountDownLatch(1);
        var entered = new CountDownLatch(1);
        var assertion = new AtomicReference<Throwable>();
        hook.action = (id, outcome) -> {
            try {
                assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
                assertThat(inbox(id)).isOne();
                assertState(event);
                assertThat(outcome).isEqualTo(ProcessingOutcome.APPLIED);
            } catch (Throwable failure) { assertion.set(failure); }
            entered.countDown();
            waitForRelease();
        };
        container = start(1, workflowKafkaListenerContainerFactory);
        long offset = send(0, event);
        try {
            assertThat(entered.await(15, TimeUnit.SECONDS)).isTrue();
            assertThat(assertion.get()).isNull();
            assertThat(committedOffset(0)).isLessThanOrEqualTo(offset);
        } finally { hook.release.countDown(); }
        committed(0, offset + 1);
        hook.action = (id, outcome) -> {};
        long duplicate = send(0, event);
        committed(0, duplicate + 1);
        assertThat(inbox(event.id())).isOne();
        assertState(event);
    }

    @Test void actualCommitFailedCallbackCannotSkipAndDuplicateCommitsBeforeFollower() throws Exception {
        var config = new HashMap<>(workflowConsumerFactory.getConfigurationProperties());
        var factory = new DefaultKafkaConsumerFactory<String, String>(config);
        factory.addPostProcessor(consumer -> {
            @SuppressWarnings("unchecked")
            Consumer<String, String> proxy = (Consumer<String, String>) Proxy.newProxyInstance(
                    Consumer.class.getClassLoader(), new Class<?>[] {Consumer.class}, (object, method, args) -> {
                        if (method.getName().equals("commitSync") && failCommit.compareAndSet(true, false)) {
                            commitFailures.incrementAndGet();
                            throw new CommitFailedException("injected offset commit failure");
                        }
                        try { return method.invoke(consumer, args); }
                        catch (InvocationTargetException ex) { throw ex.getCause(); }
                    });
            return proxy;
        });
        var configured = new ConsumerConfiguration().workflowKafkaListenerContainerFactory(factory, failures);
        var event = event();
        var follower = event();
        hook.release = new CountDownLatch(1);
        var duplicateEntered = new CountDownLatch(1);
        hook.action = (id, outcome) -> {
            if (id.equals(event.id()) && outcome == ProcessingOutcome.APPLIED) failCommit.set(true);
            if (id.equals(event.id()) && outcome == ProcessingOutcome.DUPLICATE) {
                duplicateEntered.countDown();
                waitForRelease();
            }
        };
        container = start(1, configured);
        long offset = send(0, event);
        long last = send(0, follower);
        try {
            assertThat(duplicateEntered.await(20, TimeUnit.SECONDS)).isTrue();
            assertThat(commitFailures.get()).isOne();
            assertThat(committedOffset(0)).isLessThanOrEqualTo(offset);
            assertThat(inbox(event.id())).isOne();
            assertThat(inbox(follower.id())).isZero();
        } finally { hook.release.countDown(); }
        applied(follower);
        committed(0, last + 1);
        assertState(event);
    }

    @Test void reconnectAfterBrokerRestartContinuesAtCommittedOffsets() throws Exception {
        container = start(1, workflowKafkaListenerContainerFactory);
        var first = event();
        long offset = send(0, first);
        applied(first);
        committed(0, offset + 1);
        String id = KAFKA.getContainerId();
        KAFKA.getDockerClient().stopContainerCmd(id).withTimeout(10).exec();
        try {
            assertThat(inbox(first.id())).isOne();
        } finally { KAFKA.getDockerClient().startContainerCmd(id).exec(); }
        await().atMost(Duration.ofSeconds(60)).ignoreExceptions().untilAsserted(() ->
                assertThat(admin.describeCluster().nodes().get(5, TimeUnit.SECONDS)).isNotEmpty());
        var next = event();
        long after = send(0, next);
        applied(next);
        committed(0, after + 1);
        assertThat(inbox(first.id())).isOne();
        assertThat(assigned()).isEqualTo(3);
    }

    private ConcurrentMessageListenerContainer<String, String> start(int concurrency,
            ConcurrentKafkaListenerContainerFactory<String, String> factory) {
        var result = factory.createContainer(ConsumerConfiguration.TOPIC);
        result.setConcurrency(concurrency);
        result.getContainerProperties().setGroupId(group);
        result.getContainerProperties().setMessageListener((MessageListener<String, String>) listener::onRecord);
        containers.add(result);
        result.start();
        return result;
    }

    private long send(int partition, Event event) throws Exception {
        return send(partition, event.order().toString(), event.body().getBytes(StandardCharsets.UTF_8));
    }
    private long send(int partition, String key, byte[] value) throws Exception {
        return producer.send(new ProducerRecord<>(p0.topic(), partition, key, value)).get(20, TimeUnit.SECONDS).offset();
    }
    private long committedOffset(int partition) throws Exception {
        var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        var offset = offsets.get(new TopicPartition(p0.topic(), partition));
        return offset == null ? -1 : offset.offset();
    }
    private void committed(int partition, long offset) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(committedOffset(partition)).isEqualTo(offset));
    }
    private void noSkip(long poison, Event follower) throws Exception {
        assertThat(committedOffset(0)).isLessThanOrEqualTo(poison);
        assertThat(inbox(follower.id())).isZero();
    }
    private int assigned() throws Exception {
        return admin.describeConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS).get(group)
                .members().stream().mapToInt(member -> member.assignment().topicPartitions().size()).sum();
    }
    private Set<String> memberIds() throws Exception {
        var description = admin.describeConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS).get(group);
        assertThat(description.state()).isEqualTo(org.apache.kafka.common.ConsumerGroupState.STABLE);
        return description.members().stream().map(MemberDescription::consumerId)
                .collect(java.util.stream.Collectors.toSet());
    }
    private double blockedCount() {
        return meters.find("consumer.blocked").tag("partition", "0").counters().stream()
                .mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
    }
    private double retryCount() {
        return meters.find("consumer.retry").tag("partition", "0").counters().stream()
                .mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
    }
    private int inbox(UUID event) {
        return jdbc.queryForObject("SELECT count(*) FROM inventory_event_inbox WHERE event_id=?", Integer.class, event);
    }
    private void applied(Event event) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(inbox(event.id())).isOne();
            assertState(event);
        });
    }
    private void waitForRelease() {
        try {
            if (!hook.release.await(25, TimeUnit.SECONDS)) throw new IllegalStateException("hook timed out");
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("hook interrupted"); }
    }
    private Event event() throws Exception {
        UUID order = UUID.randomUUID();
        UUID event = UUID.randomUUID();
        try (var input = getClass().getResourceAsStream("/contracts/events/orders/v1/order-placed.json")) {
            ObjectNode body = (ObjectNode) mapper.readTree(input);
            body.put("orderId", order.toString());
            body.put("eventId", event.toString());
            body.putArray("lines").addObject().put("sku", order.toString()).put("quantity", 2);
            jdbc.update("INSERT INTO stock VALUES (?, 10)", order.toString());
            return new Event(order, event, body.toString());
        }
    }
    private void assertState(Event event) {
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_result_outbox WHERE order_id=?",
                Integer.class, event.order())).isOne();
        assertThat(jdbc.queryForObject("SELECT available_quantity FROM stock WHERE sku=?",
                Integer.class, event.order().toString())).isEqualTo(8);
    }
    record Event(UUID order, UUID id, String body) {}

    static class Gate implements ConsumerCommitHook {
        volatile ConsumerCommitHook action = (id, outcome) -> {};
        volatile CountDownLatch release = new CountDownLatch(0);
        @Override public void afterDatabaseCommit(UUID id, ProcessingOutcome outcome) { action.afterDatabaseCommit(id, outcome); }
    }
    @TestConfiguration(proxyBeanMethods = false)
    static class Hooks {
        @Bean @org.springframework.context.annotation.Primary Gate gate() { return new Gate(); }
    }
}
