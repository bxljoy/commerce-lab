package com.commercelab.inventory.api;

import com.commercelab.inventory.domain.InvalidReservationException;
import com.commercelab.inventory.domain.Reservation;
import com.commercelab.inventory.domain.ReservationLine;
import com.commercelab.inventory.domain.StockItem;
import com.commercelab.inventory.domain.StockUnavailableException;
import com.commercelab.inventory.generated.api.InventoryApi;
import com.commercelab.inventory.generated.model.ReservationLineResponse;
import com.commercelab.inventory.generated.model.ReservationResponse;
import com.commercelab.inventory.generated.model.ReserveInventoryLineRequest;
import com.commercelab.inventory.generated.model.ReserveInventoryRequest;
import com.commercelab.inventory.generated.model.StockResponse;
import com.commercelab.inventory.service.InventoryService;
import com.commercelab.inventory.service.ReservationAttemptResult;
import com.commercelab.inventory.service.ReserveInventoryCommand;
import com.commercelab.inventory.web.CorrelationIdFilter;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.time.ZoneOffset;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
public class InventoryApiController implements InventoryApi {

    private final InventoryService service;
    private final HttpServletRequest servletRequest;

    public InventoryApiController(InventoryService service, HttpServletRequest servletRequest) {
        this.service = service;
        this.servletRequest = servletRequest;
    }

    @Override
    public ResponseEntity<ReservationResponse> reserveInventory(ReserveInventoryRequest request) {
        servletRequest.setAttribute(CorrelationIdFilter.ORDER_ID_ATTRIBUTE, request.getOrderId());
        var accepted = accepted(service.reserve(toCommand(request)));
        Reservation reservation = accepted.reservation();
        return ResponseEntity.status(accepted.created() ? 201 : 200)
                .location(URI.create("/api/v1/reservations/" + reservation.orderId()))
                .body(toResponse(reservation));
    }

    @Override
    public ResponseEntity<ReservationResponse> getReservation(UUID orderId) {
        servletRequest.setAttribute(CorrelationIdFilter.ORDER_ID_ATTRIBUTE, orderId);
        return ResponseEntity.ok(toResponse(accepted(service.getAttempt(orderId)).reservation()));
    }

    private static ReservationAttemptResult.Accepted accepted(ReservationAttemptResult result) {
        // The service transaction has committed before a business rejection becomes HTTP 409.
        if (result instanceof ReservationAttemptResult.Rejected rejected) {
            throw new StockUnavailableException(rejected.unavailable());
        }
        return (ReservationAttemptResult.Accepted) result;
    }

    @Override
    public ResponseEntity<ReservationResponse> releaseReservation(UUID orderId) {
        servletRequest.setAttribute(CorrelationIdFilter.ORDER_ID_ATTRIBUTE, orderId);
        return ResponseEntity.ok(toResponse(service.release(orderId)));
    }

    @Override
    public ResponseEntity<StockResponse> getStock(String sku) {
        StockItem stock = service.getStock(sku);
        return ResponseEntity.ok(new StockResponse(stock.sku(), stock.availableQuantity()));
    }

    private static ReserveInventoryCommand toCommand(ReserveInventoryRequest request) {
        return new ReserveInventoryCommand(
                request.getOrderId(),
                request.getLines().stream()
                        .map(InventoryApiController::toCommandLine)
                        .toList());
    }

    private static ReserveInventoryCommand.Line toCommandLine(
            ReserveInventoryLineRequest line) {
        if (line == null) {
            throw new InvalidReservationException("reservation line is required");
        }
        return new ReserveInventoryCommand.Line(line.getSku(), line.getQuantity());
    }

    private static ReservationResponse toResponse(Reservation reservation) {
        ReservationResponse response = new ReservationResponse()
                .orderId(reservation.orderId())
                .status(ReservationResponse.StatusEnum.fromValue(reservation.status().name()))
                .createdAt(reservation.reservedAt().atOffset(ZoneOffset.UTC));
        reservation.lines().forEach(line -> response.addLinesItem(toResponse(line)));
        if (reservation.releasedAt() != null) {
            response.releasedAt(reservation.releasedAt().atOffset(ZoneOffset.UTC));
        }
        return response;
    }

    private static ReservationLineResponse toResponse(ReservationLine line) {
        return new ReservationLineResponse(line.sku(), line.quantity());
    }
}
