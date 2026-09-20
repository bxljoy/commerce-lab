package com.commercelab.inventory.service;

import com.commercelab.inventory.domain.Availability;
import com.commercelab.inventory.domain.Reservation;
import com.commercelab.inventory.domain.ReservationPayloadConflictException;
import com.commercelab.inventory.domain.ReservationLine;
import com.commercelab.inventory.domain.ReservationNotFoundException;
import com.commercelab.inventory.domain.ReservationStatus;
import com.commercelab.inventory.domain.StockItem;
import com.commercelab.inventory.domain.StockNotFoundException;
import com.commercelab.inventory.persistence.ReservationAttemptStore;
import com.commercelab.inventory.repository.ReservationRepository;
import com.commercelab.inventory.repository.StockRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryService {

    private final StockRepository stocks;
    private final ReservationRepository reservations;
    private final Clock clock;
    private final ReservationAttemptStore attempts;

    public InventoryService(
            StockRepository stocks, ReservationRepository reservations, Clock clock,
            ReservationAttemptStore attempts) {
        this.stocks = stocks;
        this.reservations = reservations;
        this.clock = clock;
        this.attempts = attempts;
    }

    @Transactional
    public ReservationAttemptResult reserve(ReserveInventoryCommand command) {
        ReservationPayload payload = ReservationPayload.from(command);
        Reservation candidate = Reservation.reserve(
                command.orderId(), command.toDomainLines(), clock.instant());
        if (!attempts.claim(command.orderId(), payload, candidate.reservedAt())) {
            var attempt = attempts.find(command.orderId()).orElseThrow();
            if (!attempt.payload().equals(payload)) {
                throw new ReservationPayloadConflictException(command.orderId());
            }
            return replay(command.orderId(), attempt, true);
        }

        List<String> skus = candidate.lines().stream()
                .map(ReservationLine::sku)
                .sorted()
                .toList();
        Map<String, StockItem> locked = stocks.lockBySkus(skus).stream()
                .collect(Collectors.toMap(StockItem::sku, Function.identity()));
        Map<String, Availability> unavailable = collectUnavailable(candidate.lines(), locked);
        if (!unavailable.isEmpty()) {
            attempts.complete(command.orderId(), unavailable);
            return new ReservationAttemptResult.Rejected(unavailable);
        }

        List<StockItem> updated = candidate.lines().stream()
                .map(line -> locked.get(line.sku()).reserve(line.quantity()))
                .toList();
        stocks.updateAll(updated);
        reservations.add(candidate);
        attempts.complete(command.orderId(), Map.of());
        return new ReservationAttemptResult.Accepted(candidate, true);
    }

    @Transactional(readOnly = true)
    public ReservationAttemptResult getAttempt(UUID orderId) {
        return replay(orderId, attempts.find(orderId)
                .orElseThrow(() -> new ReservationNotFoundException(orderId)), false);
    }

    private ReservationAttemptResult replay(UUID orderId, ReservationAttemptStore.Attempt attempt, boolean lock) {
        if ("REJECTED".equals(attempt.outcome())) {
            return new ReservationAttemptResult.Rejected(attempt.unavailable());
        }
        Reservation reservation = lock
                ? reservations.lockByOrderId(orderId).orElseThrow(() -> new ReservationNotFoundException(orderId))
                : getReservation(orderId);
        return new ReservationAttemptResult.Accepted(reservation, false);
    }

    @Transactional
    public Reservation release(UUID orderId) {
        Reservation reservation = reservations.lockByOrderId(orderId)
                .orElseThrow(() -> new ReservationNotFoundException(orderId));
        if (reservation.status() == ReservationStatus.RELEASED) {
            return reservation;
        }

        List<String> skus = reservation.lines().stream()
                .map(ReservationLine::sku)
                .sorted()
                .toList();
        Map<String, StockItem> locked = stocks.lockBySkus(skus).stream()
                .collect(Collectors.toMap(StockItem::sku, Function.identity()));
        List<StockItem> updated = reservation.lines().stream()
                .map(line -> locked.get(line.sku()).release(line.quantity()))
                .toList();
        stocks.updateAll(updated);

        Instant releasedAt = clock.instant();
        reservations.markReleased(orderId, releasedAt);
        return reservation.release(releasedAt);
    }

    @Transactional(readOnly = true)
    public Reservation getReservation(UUID orderId) {
        return reservations.findByOrderId(orderId)
                .orElseThrow(() -> new ReservationNotFoundException(orderId));
    }

    @Transactional(readOnly = true)
    public StockItem getStock(String sku) {
        return stocks.findBySku(sku)
                .orElseThrow(() -> new StockNotFoundException(sku));
    }

    private static Map<String, Availability> collectUnavailable(
            List<ReservationLine> lines, Map<String, StockItem> locked) {
        Map<String, Availability> unavailable = new LinkedHashMap<>();
        for (ReservationLine line : lines) {
            StockItem stock = locked.get(line.sku());
            int available = stock == null ? 0 : stock.availableQuantity();
            if (line.quantity() > available) {
                unavailable.put(line.sku(), new Availability(line.quantity(), available));
            }
        }
        return unavailable;
    }
}
