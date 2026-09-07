package com.commercelab.inventory.domain;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class StockUnavailableException extends RuntimeException {

    private final Map<String, Availability> unavailableSkus;

    public StockUnavailableException(Map<String, Availability> unavailableSkus) {
        super("stock unavailable for SKUs: " + unavailableSkus.keySet());
        this.unavailableSkus = Collections.unmodifiableMap(
                new LinkedHashMap<>(Objects.requireNonNull(unavailableSkus, "unavailableSkus")));
    }

    public Map<String, Availability> unavailableSkus() {
        return unavailableSkus;
    }
}
