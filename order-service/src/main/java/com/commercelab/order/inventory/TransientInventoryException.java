package com.commercelab.order.inventory;

public class TransientInventoryException extends RuntimeException {
    private final String code;

    public TransientInventoryException(String code) {
        super(code);
        this.code = code;
    }

    public String code() { return code; }
}
