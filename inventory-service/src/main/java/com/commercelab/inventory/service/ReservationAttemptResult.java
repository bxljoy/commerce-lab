package com.commercelab.inventory.service;

import com.commercelab.inventory.domain.Availability;
import com.commercelab.inventory.domain.Reservation;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public sealed interface ReservationAttemptResult {
    record Accepted(Reservation reservation, boolean created) implements ReservationAttemptResult {}

    record Rejected(Map<String, Availability> unavailable) implements ReservationAttemptResult {
        public Rejected {
            unavailable = Collections.unmodifiableMap(new LinkedHashMap<>(unavailable));
        }
    }
}
