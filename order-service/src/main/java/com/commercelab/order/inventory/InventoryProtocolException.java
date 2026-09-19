package com.commercelab.order.inventory;

public class InventoryProtocolException extends RuntimeException {
    private final String code;

    public InventoryProtocolException(String code) {
        super(code);
        this.code = code;
    }

    public String code() { return code; }
}
