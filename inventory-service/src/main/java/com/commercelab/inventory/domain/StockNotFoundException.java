package com.commercelab.inventory.domain;

public class StockNotFoundException extends RuntimeException {

    public StockNotFoundException(String sku) {
        super("stock not found for SKU: " + sku);
    }
}
