package com.commercelab.inventory.api;

import com.commercelab.inventory.domain.Reservation;
import com.commercelab.inventory.domain.ReservationLine;
import com.commercelab.inventory.domain.StockItem;
import com.commercelab.inventory.generated.api.InventoryApi;
import com.commercelab.inventory.generated.model.ReservationLineResponse;
import com.commercelab.inventory.generated.model.ReservationResponse;
import com.commercelab.inventory.generated.model.ReserveInventoryRequest;
import com.commercelab.inventory.generated.model.StockResponse;
import com.commercelab.inventory.service.InventoryService;
import com.commercelab.inventory.service.ReserveInventoryCommand;
import java.net.URI;
import java.time.ZoneOffset;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
public class InventoryApiController implements InventoryApi {

    private final InventoryService service;

    public InventoryApiController(InventoryService service) {
        this.service = service;
    }

    @Override
    public ResponseEntity<ReservationResponse> reserveInventory(ReserveInventoryRequest request) {
        Reservation reservation = service.reserve(toCommand(request));
        return ResponseEntity.created(
                        URI.create("/api/v1/reservations/" + reservation.orderId()))
                .body(toResponse(reservation));
    }

    @Override
    public ResponseEntity<ReservationResponse> getReservation(UUID orderId) {
        return ResponseEntity.ok(toResponse(service.getReservation(orderId)));
    }

    @Override
    public ResponseEntity<ReservationResponse> releaseReservation(UUID orderId) {
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
                        .map(line -> new ReserveInventoryCommand.Line(
                                line.getSku(), line.getQuantity()))
                        .toList());
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
