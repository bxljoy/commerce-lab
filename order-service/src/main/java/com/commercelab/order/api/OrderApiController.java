package com.commercelab.order.api;

import com.commercelab.order.domain.Order;
import com.commercelab.order.domain.OrderLine;
import com.commercelab.order.generated.api.OrdersApi;
import com.commercelab.order.generated.model.OrderLineResponse;
import com.commercelab.order.generated.model.OrderResponse;
import com.commercelab.order.generated.model.PlaceOrderRequest;
import com.commercelab.order.service.OrderService;
import com.commercelab.order.service.PlaceOrderCommand;
import java.net.URI;
import java.time.ZoneOffset;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

/**
 * HTTP boundary for orders. Implements the generated {@link OrdersApi} (the spec drives
 * the signatures) and returns {@link ResponseEntity}, which is why {@code @Controller} —
 * not {@code @RestController} — is sufficient: {@code HttpEntityMethodProcessor} handles
 * the body regardless of {@code @ResponseBody}. Generated DTOs are mapped to/from the
 * domain here; the service never sees a generated type.
 */
@Controller
public class OrderApiController implements OrdersApi {

    private final OrderService orderService;

    public OrderApiController(OrderService orderService) {
        this.orderService = orderService;
    }

    @Override
    public ResponseEntity<OrderResponse> placeOrder(PlaceOrderRequest request) {
        PlaceOrderCommand command = new PlaceOrderCommand(
                request.getCustomerId(),
                request.getCurrency(),
                request.getLines().stream()
                        .map(line -> new PlaceOrderCommand.Line(line.getSku(), line.getQuantity(), line.getUnitPrice()))
                        .toList());
        Order order = orderService.placeOrder(command);
        return ResponseEntity
                .created(URI.create("/api/v1/orders/" + order.id()))
                .body(toResponse(order));
    }

    @Override
    public ResponseEntity<OrderResponse> getOrder(UUID orderId) {
        return ResponseEntity.ok(toResponse(orderService.getOrder(orderId)));
    }

    private static OrderResponse toResponse(Order order) {
        OrderResponse body = new OrderResponse()
                .id(order.id())
                .status(OrderResponse.StatusEnum.fromValue(order.status().name()))
                .currency(order.currency().getCurrencyCode())
                .totalAmount(order.total().amount())
                .placedAt(order.placedAt().atOffset(ZoneOffset.UTC));
        order.lines().forEach(line -> body.addLinesItem(toLineResponse(line)));
        return body;
    }

    private static OrderLineResponse toLineResponse(OrderLine line) {
        return new OrderLineResponse()
                .sku(line.sku())
                .quantity(line.quantity())
                .unitPrice(line.unitPrice().amount())
                .lineTotal(line.lineTotal().amount());
    }
}
