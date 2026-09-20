package com.commercelab.inventory.outbox;

public class OutboxPublishException extends RuntimeException {
    private final String code;

    public OutboxPublishException(String code, Throwable cause) {
        super(code, cause);
        this.code = code;
    }

    public String code() { return code; }
}
