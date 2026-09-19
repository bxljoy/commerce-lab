package com.commercelab.inventory.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;

public final class InventoryContract {
    public static JsonNode load(String name) throws Exception {
        try (var stream = InventoryContract.class.getResourceAsStream("/contracts/inventory/v1/" + name + ".json")) {
            if (stream == null) throw new IllegalArgumentException("Missing inventory contract: " + name);
            return new ObjectMapper().readTree(stream);
        }
    }

    public static void validate(JsonNode contract, int status, JsonNode body) {
        assertThat(status).as("HTTP status").isEqualTo(contract.path("status").intValue());
        JsonNode expected = contract.path("body");
        assertThat(body.isObject()).as("response object").isTrue();
        if (status >= 400) {
            assertThat(body.path("type")).as("problem type").isEqualTo(expected.path("type"));
            assertThat(body.path("status")).as("problem status").isEqualTo(expected.path("status"));
            if (expected.has("unavailableSkus")) {
                assertThat(body.path("unavailableSkus")).as("stored stock snapshot")
                        .isEqualTo(expected.path("unavailableSkus"));
            }
            return;
        }
        assertThat(body.path("orderId")).as("orderId").isEqualTo(expected.path("orderId"));
        assertThat(body.path("status")).as("reservation status").isEqualTo(expected.path("status"));
        assertThat(body.path("lines").isArray()).as("lines array").isTrue();
        assertThat(body.path("lines").size()).as("line count").isEqualTo(expected.path("lines").size());
        for (int i = 0; i < expected.path("lines").size(); i++) {
            assertThat(body.path("lines").get(i).path("sku")).as("line sku")
                    .isEqualTo(expected.path("lines").get(i).path("sku"));
            assertThat(body.path("lines").get(i).path("quantity")).as("line quantity")
                    .isEqualTo(expected.path("lines").get(i).path("quantity"));
        }
        timestamp(body.path("createdAt"));
        if (expected.has("releasedAt")) timestamp(body.path("releasedAt"));
    }

    private static void timestamp(JsonNode value) {
        assertThat(value.isTextual()).as("timestamp string").isTrue();
        assertThatCode(() -> OffsetDateTime.parse(value.textValue())).doesNotThrowAnyException();
    }
}
