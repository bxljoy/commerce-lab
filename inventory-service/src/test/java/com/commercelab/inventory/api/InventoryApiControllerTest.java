package com.commercelab.inventory.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.commercelab.inventory.domain.Availability;
import com.commercelab.inventory.domain.InvalidReservationException;
import com.commercelab.inventory.domain.Reservation;
import com.commercelab.inventory.domain.ReservationAlreadyExistsException;
import com.commercelab.inventory.domain.ReservationLine;
import com.commercelab.inventory.domain.ReservationNotFoundException;
import com.commercelab.inventory.domain.ReservationStatus;
import com.commercelab.inventory.domain.StockItem;
import com.commercelab.inventory.domain.StockNotFoundException;
import com.commercelab.inventory.domain.StockUnavailableException;
import com.commercelab.inventory.service.InventoryService;
import com.commercelab.inventory.service.ReserveInventoryCommand;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(InventoryApiController.class)
class InventoryApiControllerTest {

    private static final UUID ORDER_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final Instant RESERVED_AT = Instant.parse("2026-09-07T12:00:00Z");
    private static final Instant RELEASED_AT = Instant.parse("2026-09-07T12:05:00Z");

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private InventoryService inventoryService;

    @Test
    void reserveReturns201WithRelativeLocationAndLinesInRequestOrder() throws Exception {
        when(inventoryService.reserve(argThat(command ->
                        command.orderId().equals(ORDER_ID)
                                && command.lines().equals(List.of(
                                        new ReserveInventoryCommand.Line("SKU-BANANA", 2),
                                        new ReserveInventoryCommand.Line("SKU-APPLE", 1))))))
                .thenReturn(reserved());

        mockMvc.perform(post("/api/v1/reservations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validBody()))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/api/v1/reservations/" + ORDER_ID))
                .andExpect(jsonPath("$.orderId").value(ORDER_ID.toString()))
                .andExpect(jsonPath("$.status").value("RESERVED"))
                .andExpect(jsonPath("$.createdAt").value("2026-09-07T12:00:00Z"))
                .andExpect(jsonPath("$.lines[0].sku").value("SKU-BANANA"))
                .andExpect(jsonPath("$.lines[1].sku").value("SKU-APPLE"));
    }

    @Test
    void duplicateSkuReturnsInvalidReservationProblem() throws Exception {
        when(inventoryService.reserve(any()))
                .thenThrow(new InvalidReservationException("duplicate SKU: SKU-APPLE"));

        mockMvc.perform(post("/api/v1/reservations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(duplicateSkuBody()))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type")
                        .value("https://commerce-lab/errors/invalid-reservation"))
                .andExpect(jsonPath("$.title").value("Invalid reservation"))
                .andExpect(jsonPath("$.detail").value("duplicate SKU: SKU-APPLE"));
    }

    @Test
    void generatedBeanValidationReturnsFieldMessageMap() throws Exception {
        String invalidBody = """
                {
                  "lines": [ { "sku": "", "quantity": 0 } ]
                }
                """;

        mockMvc.perform(post("/api/v1/reservations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidBody))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type").value("https://commerce-lab/errors/validation"))
                .andExpect(jsonPath("$.errors.orderId").exists())
                .andExpect(jsonPath("$.errors['lines[0].sku']").exists())
                .andExpect(jsonPath("$.errors['lines[0].quantity']").exists());
    }

    @Test
    void getReservationReturns200WithMappedFieldsAndLinesInOriginalOrder() throws Exception {
        when(inventoryService.getReservation(ORDER_ID)).thenReturn(reserved());

        mockMvc.perform(get("/api/v1/reservations/{orderId}", ORDER_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(ORDER_ID.toString()))
                .andExpect(jsonPath("$.status").value("RESERVED"))
                .andExpect(jsonPath("$.createdAt").value("2026-09-07T12:00:00Z"))
                .andExpect(jsonPath("$.releasedAt").doesNotExist())
                .andExpect(jsonPath("$.lines.length()").value(2))
                .andExpect(jsonPath("$.lines[0].sku").value("SKU-BANANA"))
                .andExpect(jsonPath("$.lines[0].quantity").value(2))
                .andExpect(jsonPath("$.lines[1].sku").value("SKU-APPLE"))
                .andExpect(jsonPath("$.lines[1].quantity").value(1));
    }

    @Test
    void missingReservationReturnsReservationNotFoundProblem() throws Exception {
        when(inventoryService.getReservation(ORDER_ID))
                .thenThrow(new ReservationNotFoundException(ORDER_ID));

        mockMvc.perform(get("/api/v1/reservations/{orderId}", ORDER_ID))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type")
                        .value("https://commerce-lab/errors/reservation-not-found"));
    }

    @Test
    void missingStockReturnsStockNotFoundProblem() throws Exception {
        when(inventoryService.getStock("SKU-MISSING"))
                .thenThrow(new StockNotFoundException("SKU-MISSING"));

        mockMvc.perform(get("/api/v1/stock/{sku}", "SKU-MISSING"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type").value("https://commerce-lab/errors/stock-not-found"));
    }

    @Test
    void unavailableStockReturnsConflictWithEveryUnavailableSku() throws Exception {
        Map<String, Availability> unavailable = new LinkedHashMap<>();
        unavailable.put("SKU-BANANA", new Availability(7, 5));
        unavailable.put("SKU-UNKNOWN", new Availability(2, 0));
        when(inventoryService.reserve(any())).thenThrow(new StockUnavailableException(unavailable));

        mockMvc.perform(post("/api/v1/reservations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(unavailableBody()))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type").value("https://commerce-lab/errors/stock-unavailable"))
                .andExpect(jsonPath("$.unavailableSkus.SKU-BANANA.requested").value(7))
                .andExpect(jsonPath("$.unavailableSkus.SKU-BANANA.available").value(5))
                .andExpect(jsonPath("$.unavailableSkus.SKU-UNKNOWN.requested").value(2))
                .andExpect(jsonPath("$.unavailableSkus.SKU-UNKNOWN.available").value(0));
    }

    @Test
    void existingReservationReturnsConflict() throws Exception {
        when(inventoryService.reserve(any()))
                .thenThrow(new ReservationAlreadyExistsException(ORDER_ID));

        mockMvc.perform(post("/api/v1/reservations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validBody()))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type")
                        .value("https://commerce-lab/errors/reservation-already-exists"));
    }

    @Test
    void releaseReturns200WhenAlreadyReleased() throws Exception {
        when(inventoryService.release(ORDER_ID)).thenReturn(released());

        mockMvc.perform(put("/api/v1/reservations/{orderId}/release", ORDER_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("RELEASED"))
                .andExpect(jsonPath("$.releasedAt").value("2026-09-07T12:05:00Z"));

        mockMvc.perform(put("/api/v1/reservations/{orderId}/release", ORDER_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("RELEASED"));
    }

    @Test
    void releaseMissingReservationReturnsReservationNotFoundProblem() throws Exception {
        when(inventoryService.release(ORDER_ID))
                .thenThrow(new ReservationNotFoundException(ORDER_ID));

        mockMvc.perform(put("/api/v1/reservations/{orderId}/release", ORDER_ID))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type")
                        .value("https://commerce-lab/errors/reservation-not-found"));
    }

    @Test
    void getStockReturnsAvailableQuantity() throws Exception {
        when(inventoryService.getStock("SKU-APPLE")).thenReturn(new StockItem("SKU-APPLE", 8));

        mockMvc.perform(get("/api/v1/stock/{sku}", "SKU-APPLE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sku").value("SKU-APPLE"))
                .andExpect(jsonPath("$.availableQuantity").value(8));
    }

    private static Reservation reserved() {
        return new Reservation(
                ORDER_ID,
                List.of(new ReservationLine("SKU-BANANA", 2),
                        new ReservationLine("SKU-APPLE", 1)),
                ReservationStatus.RESERVED,
                RESERVED_AT,
                null);
    }

    private static Reservation released() {
        return new Reservation(
                ORDER_ID,
                reserved().lines(),
                ReservationStatus.RELEASED,
                RESERVED_AT,
                RELEASED_AT);
    }

    private static String validBody() {
        return """
                {
                  "orderId": "11111111-1111-1111-1111-111111111111",
                  "lines": [
                    { "sku": "SKU-BANANA", "quantity": 2 },
                    { "sku": "SKU-APPLE", "quantity": 1 }
                  ]
                }
                """;
    }

    private static String duplicateSkuBody() {
        return """
                {
                  "orderId": "11111111-1111-1111-1111-111111111111",
                  "lines": [
                    { "sku": "SKU-APPLE", "quantity": 1 },
                    { "sku": "SKU-APPLE", "quantity": 2 }
                  ]
                }
                """;
    }

    private static String unavailableBody() {
        return """
                {
                  "orderId": "11111111-1111-1111-1111-111111111111",
                  "lines": [
                    { "sku": "SKU-BANANA", "quantity": 7 },
                    { "sku": "SKU-UNKNOWN", "quantity": 2 }
                  ]
                }
                """;
    }
}
