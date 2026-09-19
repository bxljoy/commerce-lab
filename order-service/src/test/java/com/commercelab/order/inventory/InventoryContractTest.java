package com.commercelab.order.inventory;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

class InventoryContractTest {
    static final UUID ID = UUID.fromString("11111111-1111-4111-8111-111111111111");
    static final InventoryRequest REQUEST = new InventoryRequest(ID, List.of(new InventoryLine("A", 2)));
    InventoryContractFixture fixture;
    org.apache.hc.client5.http.impl.classic.CloseableHttpClient client;
    InventoryGateway gateway;

    @BeforeEach void setup() {
        fixture = new InventoryContractFixture(false);
        var config = new InventoryClientConfiguration();
        client = config.inventoryHttpClient(500, 500, 2000, 2, 1);
        gateway = new RestInventoryGateway(RestClient.builder().baseUrl(fixture.url())
                .requestFactory(new HttpComponentsClientHttpRequestFactory(client)).build(),
                new ObjectMapper(), config.inventoryCircuitBreaker());
    }

    @AfterEach void close() throws Exception {
        try { client.close(); } finally { fixture.close(); }
    }

    @ParameterizedTest @ValueSource(strings = {"accepted", "replay"})
    void acceptedAndReplayDeserializeToReserved(String name) throws Exception {
        fixture.respond(name);
        assertThat(gateway.reserve(REQUEST, "contract")).isEqualTo(new InventoryOutcome.Reserved(ID, REQUEST.lines()));
        if (name.equals("replay")) {
            assertThat(gateway.find(ID, "contract")).isEqualTo(new InventoryOutcome.Reserved(ID, REQUEST.lines()));
        }
    }

    @Test void releasedReplayNeverMeansReserved() throws Exception {
        fixture.respond("released");
        var expected = new InventoryOutcome.Released(ID, REQUEST.lines());
        assertThat(gateway.reserve(REQUEST, "contract")).isEqualTo(expected);
        assertThat(gateway.find(ID, "contract")).isEqualTo(expected);
    }

    @Test void stockRejectionDeserializesItsStoredSnapshot() throws Exception {
        fixture.respond("stock-rejected");
        var expected = new InventoryOutcome.Rejected(Map.of("A", new StockShortage(2, 0)));
        assertThat(gateway.reserve(REQUEST, "contract")).isEqualTo(expected);
        assertThat(gateway.find(ID, "contract")).isEqualTo(expected);
    }

    @Test void missingOnlyMeansAbsenceForLookup() throws Exception {
        fixture.respond("missing");
        assertThat(gateway.find(ID, "contract")).isEqualTo(new InventoryOutcome.Missing());
        assertThatThrownBy(() -> gateway.reserve(REQUEST, "contract")).isInstanceOf(InventoryProtocolException.class);
    }

    @Test void conflictIsProtocolFailureNotStockRejection() throws Exception {
        fixture.respond("conflict");
        assertThatThrownBy(() -> gateway.reserve(REQUEST, "contract"))
                .isInstanceOfSatisfying(InventoryProtocolException.class,
                        ex -> assertThat(ex.code()).isEqualTo("INVENTORY_PAYLOAD_CONFLICT"));
        assertThatThrownBy(() -> gateway.find(ID, "contract")).isInstanceOf(InventoryProtocolException.class);
    }

    @ParameterizedTest @ValueSource(strings = {"missing-orderId", "quantity-text", "wrong-problem-type"})
    void actualGatewayRejectsMutatedConsumerFixtures(String mutation) throws Exception {
        var contract = InventoryContractFixture.load(mutation.equals("wrong-problem-type") ? "stock-rejected" : "accepted");
        ObjectNode body = (ObjectNode) contract.path("body");
        switch (mutation) {
            case "missing-orderId" -> body.remove("orderId");
            case "quantity-text" -> ((ObjectNode) body.at("/lines/0")).put("quantity", "2");
            case "wrong-problem-type" -> body.put("type", "https://other.example/stock-unavailable");
        }
        fixture.respond(contract);
        assertThatThrownBy(() -> gateway.reserve(REQUEST, "contract")).isInstanceOf(InventoryProtocolException.class);
    }
}
