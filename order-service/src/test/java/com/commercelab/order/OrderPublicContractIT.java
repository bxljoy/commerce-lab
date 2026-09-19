package com.commercelab.order;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.commercelab.order.domain.Order;
import com.commercelab.order.domain.OrderStatus;
import com.commercelab.order.repository.OrderRepository;
import com.commercelab.order.persistence.OrderRequestStore;
import com.commercelab.order.service.OrderPayload;
import com.commercelab.order.service.PlaceOrderCommand;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class OrderPublicContractIT extends AbstractPostgresIntegrationTest {
    static final ObjectMapper JSON = new ObjectMapper();
    static final String REQUEST = """
            {"customerId":"contract-customer","currency":"EUR","lines":[{"sku":"A","quantity":2,"unitPrice":1}]}
            """;

    @Autowired MockMvc mvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired OrderRepository orders;
    @Autowired OrderRequestStore requests;
    @Autowired org.springframework.transaction.PlatformTransactionManager transactionManager;

    @BeforeEach void reset() {
        jdbc.execute("TRUNCATE order_outbox, order_requests, order_lines, orders CASCADE");
    }

    @Test void creationAndReplayArePending() throws Exception {
        JsonNode created = check("pending", send(REQUEST));
        JsonNode replayed = check("pending", send(REQUEST));
        assertThat(replayed.path("id")).as("replayed identity").isEqualTo(created.path("id"));
        assertThat(replayed.path("placedAt")).as("original placement").isEqualTo(created.path("placedAt"));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_outbox", Long.class)).isEqualTo(1);
    }

    @ParameterizedTest @ValueSource(strings = {"confirmed", "rejected"})
    void historicalTerminalReplayCreatesNoEvent(String scenario) throws Exception {
        var payload = OrderPayload.from(new PlaceOrderCommand("contract-customer", "EUR", java.util.List.of(
                new PlaceOrderCommand.Line("A", 2, java.math.BigDecimal.ONE))));
        var historical = new Order(UUID.randomUUID(), payload.customerId(),
                scenario.equals("confirmed") ? OrderStatus.CONFIRMED : OrderStatus.REJECTED,
                payload.lines(), java.time.Instant.parse("2026-09-19T12:00:00Z"),
                scenario.equals("rejected") ? "STOCK_UNAVAILABLE" : null, null);
        new org.springframework.transaction.support.TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
            orders.add(historical);
            requests.insert("public-contract", historical.id(), payload, "historical");
        });
        for (int i = 0; i < 2; i++) {
            var replay = check("replay-" + scenario, send(REQUEST));
            assertThat(replay.path("id").textValue()).isEqualTo(historical.id().toString());
            assertThat(OffsetDateTime.parse(replay.path("placedAt").textValue()).toInstant())
                    .isEqualTo(historical.placedAt());
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM orders", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_requests", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_outbox", Long.class)).isZero();
    }

    @Test void changedValidPayloadHasKeyConflictProblem() throws Exception {
        check("pending", send(REQUEST));
        var changed = (com.fasterxml.jackson.databind.node.ObjectNode) JSON.readTree(REQUEST);
        changed.put("customerId", "different-customer");
        check("conflict", send(changed.toString()));
    }

    private MockHttpServletResponse send(String body) throws Exception {
        return mvc.perform(post("/api/v1/orders").header("Idempotency-Key", "public-contract")
                .contentType("application/json").content(body)).andReturn().getResponse();
    }

    private JsonNode check(String name, MockHttpServletResponse response) throws Exception {
        JsonNode contract;
        try (var stream = getClass().getResourceAsStream("/contracts/orders/v1/" + name + ".json")) {
            assertThat(stream).as("consumer fixture " + name).isNotNull();
            contract = JSON.readTree(stream);
        }
        JsonNode body = JSON.readTree(response.getContentAsString());
        JsonNode expected = contract.path("body");
        assertThat(response.getStatus()).as("HTTP status").isEqualTo(contract.path("status").intValue());
        if (response.getStatus() >= 400) {
            assertThat(body.path("type")).as("problem type").isEqualTo(expected.path("type"));
            assertThat(body.path("status")).as("problem status").isEqualTo(expected.path("status"));
            return body;
        }
        assertThat(body.path("id").isTextual()).as("order id string").isTrue();
        assertThatCode(() -> UUID.fromString(body.path("id").textValue())).doesNotThrowAnyException();
        assertThat(body.path("placedAt").isTextual()).as("placement timestamp string").isTrue();
        assertThatCode(() -> OffsetDateTime.parse(body.path("placedAt").textValue())).doesNotThrowAnyException();
        for (String field : new String[] {"status", "currency", "rejectionReason", "recoveryIssue"}) {
            assertThat(body.path(field)).as(field).isEqualTo(expected.path(field));
        }
        number(body.path("totalAmount"), expected.path("totalAmount"));
        assertThat(body.path("lines").isArray()).as("lines array").isTrue();
        assertThat(body.path("lines").size()).as("line count").isEqualTo(expected.path("lines").size());
        for (int i = 0; i < expected.path("lines").size(); i++) {
            JsonNode actualLine = body.path("lines").get(i);
            JsonNode expectedLine = expected.path("lines").get(i);
            assertThat(actualLine.path("sku")).as("line sku").isEqualTo(expectedLine.path("sku"));
            assertThat(actualLine.path("quantity")).as("integer quantity").isEqualTo(expectedLine.path("quantity"));
            number(actualLine.path("unitPrice"), expectedLine.path("unitPrice"));
            number(actualLine.path("lineTotal"), expectedLine.path("lineTotal"));
        }
        assertThat(response.getHeader("Location")).as("Location")
                .isEqualTo(contract.at("/headers/Location").textValue().replace("{id}", body.path("id").textValue()));
        assertThat(response.getHeader("Retry-After")).as("Retry-After")
                .isEqualTo(contract.at("/headers/Retry-After").textValue());
        return body;
    }

    private void number(JsonNode actual, JsonNode expected) {
        assertThat(actual.isNumber()).as("JSON monetary number").isTrue();
        assertThat(actual.decimalValue()).isEqualByComparingTo(expected.decimalValue());
    }
}
