package com.commercelab.inventory.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.commercelab.inventory.AbstractPostgresIntegrationTest;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class InventoryApiIT extends AbstractPostgresIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void resetDatabase() {
        jdbcTemplate.execute(
                "TRUNCATE TABLE inventory_reservation_attempts, inventory_reservation_lines, inventory_reservations, stock CASCADE");
        jdbcTemplate.update(
                "INSERT INTO stock (sku, available_quantity) VALUES (?, ?)", "SKU-APPLE", 10);
        jdbcTemplate.update(
                "INSERT INTO stock (sku, available_quantity) VALUES (?, ?)", "SKU-BANANA", 5);
    }

    @Test
    void reserveGetAndDoubleReleaseThroughRealBeansAndPostgres() throws Exception {
        UUID orderId = UUID.randomUUID();
        String body = """
                {
                  "orderId": "%s",
                  "lines": [
                    { "sku": "SKU-BANANA", "quantity": 2 },
                    { "sku": "SKU-APPLE", "quantity": 3 }
                  ]
                }
                """.formatted(orderId);

        mockMvc.perform(post("/api/v1/reservations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/api/v1/reservations/" + orderId))
                .andExpect(jsonPath("$.orderId").value(orderId.toString()))
                .andExpect(jsonPath("$.status").value("RESERVED"));

        mockMvc.perform(get("/api/v1/reservations/{orderId}", orderId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.lines.length()").value(2))
                .andExpect(jsonPath("$.lines[0].sku").value("SKU-BANANA"))
                .andExpect(jsonPath("$.lines[0].quantity").value(2))
                .andExpect(jsonPath("$.lines[1].sku").value("SKU-APPLE"))
                .andExpect(jsonPath("$.lines[1].quantity").value(3));

        assertStock("SKU-BANANA", 3);
        assertStock("SKU-APPLE", 7);

        mockMvc.perform(put("/api/v1/reservations/{orderId}/release", orderId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("RELEASED"))
                .andExpect(jsonPath("$.releasedAt").isNotEmpty());

        mockMvc.perform(put("/api/v1/reservations/{orderId}/release", orderId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("RELEASED"));

        assertStock("SKU-BANANA", 5);
        assertStock("SKU-APPLE", 10);
    }

    private void assertStock(String sku, int availableQuantity) throws Exception {
        mockMvc.perform(get("/api/v1/stock/{sku}", sku))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sku").value(sku))
                .andExpect(jsonPath("$.availableQuantity").value(availableQuantity));
    }
}
