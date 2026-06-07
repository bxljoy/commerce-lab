package com.commercelab.order.api;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.commercelab.order.domain.Money;
import com.commercelab.order.domain.Order;
import com.commercelab.order.domain.OrderLine;
import com.commercelab.order.domain.OrderNotFoundException;
import com.commercelab.order.service.OrderService;
import com.commercelab.order.web.ApiExceptionHandler;
import java.math.BigDecimal;
import java.util.Currency;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Web-layer slice test: boots only the controller + MVC infrastructure (no service, no
 * repository), with {@link OrderService} mocked. Verifies the HTTP boundary — status
 * codes, Location header, JSON shape, and RFC-7807 problem responses.
 */
@WebMvcTest(OrderApiController.class)
@Import(ApiExceptionHandler.class)
class OrderApiControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private OrderService orderService;

    private static Order sampleOrder() {
        return Order.place("cust-1", List.of(
                new OrderLine("SKU-1", 2, new Money(new BigDecimal("9.99"), Currency.getInstance("EUR")))));
    }

    @Test
    void placeOrderReturns201WithLocationAndBody() throws Exception {
        Order order = sampleOrder();
        when(orderService.placeOrder(any())).thenReturn(order);

        String body = """
                {
                  "customerId": "cust-1",
                  "currency": "EUR",
                  "lines": [ { "sku": "SKU-1", "quantity": 2, "unitPrice": 9.99 } ]
                }
                """;

        mockMvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", containsString("/api/v1/orders/" + order.id())))
                .andExpect(jsonPath("$.status").value("PLACED"))
                .andExpect(jsonPath("$.currency").value("EUR"))
                .andExpect(jsonPath("$.totalAmount").value(19.98))
                .andExpect(jsonPath("$.lines[0].lineTotal").value(19.98));
    }

    @Test
    void placeOrderReturns400ProblemDetailOnInvalidBody() throws Exception {
        String invalid = """
                { "currency": "EU", "lines": [] }
                """; // missing customerId, malformed currency, empty lines

        mockMvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content(invalid))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Validation failed"))
                .andExpect(jsonPath("$.errors").exists());
    }

    @Test
    void getOrderReturns200() throws Exception {
        Order order = sampleOrder();
        when(orderService.getOrder(order.id())).thenReturn(order);

        mockMvc.perform(get("/api/v1/orders/{id}", order.id()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(order.id().toString()))
                .andExpect(jsonPath("$.status").value("PLACED"));
    }

    @Test
    void getOrderReturns404ProblemDetailWhenMissing() throws Exception {
        UUID missing = UUID.randomUUID();
        when(orderService.getOrder(missing)).thenThrow(new OrderNotFoundException(missing));

        mockMvc.perform(get("/api/v1/orders/{id}", missing))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Order not found"));
    }
}
