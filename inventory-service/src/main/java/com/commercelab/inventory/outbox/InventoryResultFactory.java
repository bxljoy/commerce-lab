package com.commercelab.inventory.outbox;

import com.commercelab.inventory.domain.ReservationStatus;
import com.commercelab.inventory.events.EventProtocolException;
import com.commercelab.inventory.events.InventoryResult;
import com.commercelab.inventory.events.OrderPlaced;
import com.commercelab.inventory.service.ReservationAttemptResult;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class InventoryResultFactory {
    private final Clock clock;

    public InventoryResultFactory(Clock clock) {
        this.clock = clock;
    }

    public InventoryResult create(OrderPlaced event, ReservationAttemptResult result) {
        String type;
        String reason = null;
        List<InventoryResult.Shortage> shortages = null;
        switch (result) {
            case ReservationAttemptResult.Accepted accepted -> {
                if (accepted.reservation().status() != ReservationStatus.RESERVED)
                    throw new EventProtocolException("RESERVATION_RELEASED");
                type = "InventoryReserved";
            }
            case ReservationAttemptResult.Rejected rejected -> {
                type = "InventoryRejected";
                reason = "INSUFFICIENT_STOCK";
                shortages = event.lines().stream().filter(line -> rejected.unavailable().containsKey(line.sku()))
                        .map(line -> {
                            var unavailable = rejected.unavailable().get(line.sku());
                            return new InventoryResult.Shortage(line.sku(), unavailable.requested(), unavailable.available());
                        }).toList();
            }
        }
        return new InventoryResult(UUID.randomUUID(), type, 1, clock.instant(), event.orderId(),
                event.correlationId(), event.eventId(), event.lines().stream()
                        .map(line -> new InventoryResult.Line(line.sku(), line.quantity())).toList(), reason, shortages);
    }
}
