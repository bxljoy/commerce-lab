package com.commercelab.order.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/**
 * Full-stack vertical slice with <em>real</em> beans wired by Spring
 * (controller → service → in-memory repository). Unlike the {@code @WebMvcTest} slice,
 * which mocks {@code OrderService}, this locks in the Phase 1 demo path end-to-end
 * in-process: place an order, then retrieve the same order back.
 */
@SpringBootTest
@AutoConfigureMockMvc
class OrderApiIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void placeThenGetOrderThroughRealBeans() throws Exception {
        String body = """
                {
                  "customerId": "cust-int",
                  "currency": "EUR",
                  "lines": [
                    { "sku": "SKU-1", "quantity": 2, "unitPrice": 9.99 },
                    { "sku": "SKU-2", "quantity": 1, "unitPrice": 4.00 }
                  ]
                }
                """;

        MvcResult created = mockMvc.perform(post("/api/v1/orders")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.status").value("PLACED"))
                .andExpect(jsonPath("$.totalAmount").value(23.98))
                .andReturn();

        String id = JsonPath.read(created.getResponse().getContentAsString(), "$.id");

        // retrieve the persisted order back through the real repository
        mockMvc.perform(get("/api/v1/orders/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.currency").value("EUR"))
                .andExpect(jsonPath("$.totalAmount").value(23.98))
                .andExpect(jsonPath("$.lines.length()").value(2));
    }
}
