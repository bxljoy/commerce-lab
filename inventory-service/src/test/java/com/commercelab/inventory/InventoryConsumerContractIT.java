package com.commercelab.inventory;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

import com.commercelab.inventory.contract.InventoryContract;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = "inventory.outbox.enabled=false")
@AutoConfigureMockMvc
class InventoryConsumerContractIT extends AbstractPostgresIntegrationTest {
    static final String ID = "11111111-1111-4111-8111-111111111111";
    static final String PATH = "/api/v1/reservations/" + ID;
    static final ObjectMapper JSON = new ObjectMapper();
    @Autowired MockMvc mvc;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach void reset() {
        jdbc.execute("TRUNCATE inventory_reservation_attempts, inventory_reservations, inventory_reservation_lines, stock CASCADE");
        jdbc.update("INSERT INTO stock VALUES ('A', 10)");
    }

    @Test void acceptedReplayGetAndReleasedReplayMeetConsumerContract() throws Exception {
        check("accepted", reserve(2));
        check("replay", reserve(2));
        check("replay", mvc.perform(get(PATH)).andReturn().getResponse());
        check("released", mvc.perform(put(PATH + "/release")).andReturn().getResponse());
        check("released", reserve(2));
        check("released", mvc.perform(get(PATH)).andReturn().getResponse());
    }

    @Test void storedStockRejectionSurvivesReplenishmentForPostAndGet() throws Exception {
        jdbc.update("UPDATE stock SET available_quantity = 0");
        check("stock-rejected", reserve(2));
        jdbc.update("UPDATE stock SET available_quantity = 10");
        check("stock-rejected", reserve(2));
        check("stock-rejected", mvc.perform(get(PATH)).andReturn().getResponse());
    }

    @Test void unknownAttemptAndChangedPayloadHaveDistinctProblems() throws Exception {
        check("missing", mvc.perform(get(PATH)).andReturn().getResponse());
        check("accepted", reserve(2));
        check("conflict", reserve(3));
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing-orderId", "quantity-text", "wrong-problem-type"})
    void mutatedRealProducerResponseIsRejected(String mutation) throws Exception {
        boolean rejection = mutation.equals("wrong-problem-type");
        if (rejection) jdbc.update("UPDATE stock SET available_quantity = 0");
        String name = rejection ? "stock-rejected" : "accepted";
        var response = reserve(2);
        check(name, response);
        ObjectNode body = (ObjectNode) JSON.readTree(response.getContentAsString());
        switch (mutation) {
            case "missing-orderId" -> body.remove("orderId");
            case "quantity-text" -> ((ObjectNode) body.at("/lines/0")).put("quantity", "2");
            case "wrong-problem-type" -> body.put("type", "https://other.example/stock-unavailable");
        }
        var contract = InventoryContract.load(name);
        // Opt-in evidence mode deliberately lets the assertion escape and fails Maven.
        if (Boolean.getBoolean("contract.mutationEvidence")) {
            InventoryContract.validate(contract, response.getStatus(), body);
        } else {
            assertThatThrownBy(() -> InventoryContract.validate(contract, response.getStatus(), body))
                    .isInstanceOf(AssertionError.class);
        }
    }

    private MockHttpServletResponse reserve(int quantity) throws Exception {
        return mvc.perform(post("/api/v1/reservations").contentType("application/json")
                .content("{\"orderId\":\"" + ID + "\",\"lines\":[{\"sku\":\"A\",\"quantity\":" + quantity + "}]}"))
                .andReturn().getResponse();
    }

    private void check(String name, MockHttpServletResponse response) throws Exception {
        InventoryContract.validate(InventoryContract.load(name), response.getStatus(),
                JSON.readTree(response.getContentAsString()));
    }
}
