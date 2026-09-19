package com.commercelab.inventory.contract;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class InventoryContractTest {
    @ParameterizedTest
    @ValueSource(strings = {"missing-orderId", "quantity-text", "wrong-problem-type"})
    void rejectsConsumerBreakingMutations(String mutation) throws Exception {
        var contract = InventoryContract.load(mutation.equals("wrong-problem-type") ? "stock-rejected" : "accepted");
        ObjectNode body = contract.get("body").deepCopy();
        switch (mutation) {
            case "missing-orderId" -> body.remove("orderId");
            case "quantity-text" -> ((ObjectNode) body.at("/lines/0")).put("quantity", "2");
            case "wrong-problem-type" -> body.put("type", "https://other.example/stock-unavailable");
        }
        assertThatThrownBy(() -> InventoryContract.validate(contract, contract.get("status").intValue(), body))
                .isInstanceOf(AssertionError.class);
    }
}
