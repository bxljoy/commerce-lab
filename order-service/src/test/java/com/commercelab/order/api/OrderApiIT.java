package com.commercelab.order.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.commercelab.order.AbstractPostgresIntegrationTest;
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
 * (controller → service → JPA repository → Postgres via Testcontainers). Unlike the
 * {@code @WebMvcTest} slice, which mocks {@code OrderService}, this locks in the demo
 * path end-to-end: place an order over HTTP, then retrieve the same order back.
 */
@SpringBootTest
@AutoConfigureMockMvc
class OrderApiIT extends AbstractPostgresIntegrationTest {

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
                        .header("Idempotency-Key", "api-create")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Retry-After", "5"))
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.status").value("PENDING_INVENTORY"))
                .andExpect(jsonPath("$.totalAmount").value(23.98))
                .andReturn();

        String id = JsonPath.read(created.getResponse().getContentAsString(), "$.id");

        // retrieve the persisted order back through the real repository
        mockMvc.perform(get("/api/v1/orders/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.currency").value("EUR"))
                .andExpect(jsonPath("$.totalAmount").value(23.98))
                .andExpect(jsonPath("$.lines.length()").value(2))
                .andExpect(jsonPath("$.lines[0].sku").value("SKU-1"))
                .andExpect(jsonPath("$.lines[1].sku").value("SKU-2"));
    }

    @Test
    void rejectsPriceThatPostgresWouldOtherwiseRound() throws Exception {
        String body = """
                {
                  "customerId": "cust-int",
                  "currency": "EUR",
                  "lines": [
                    { "sku": "SKU-1", "quantity": 1, "unitPrice": 9.99999 }
                  ]
                }
                """;

        mockMvc.perform(post("/api/v1/orders")
                        .header("Idempotency-Key", "api-rounding")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Invalid order"))
                .andExpect(jsonPath("$.detail").value(
                        "unitPrice supports at most 4 fractional digits, was 9.99999"));
    }

    @Test
    void rejectsPriceOutsidePostgresNumericRange() throws Exception {
        String body = """
                {
                  "customerId": "cust-int",
                  "currency": "EUR",
                  "lines": [
                    { "sku": "SKU-1", "quantity": 1, "unitPrice": 1000000000000000 }
                  ]
                }
                """;

        mockMvc.perform(post("/api/v1/orders")
                        .header("Idempotency-Key", "api-range")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Validation failed"))
                .andExpect(jsonPath("$.errors['lines[0].unitPrice']").exists());
    }
}
