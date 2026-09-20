package com.commercelab.order.events;

/** Metadata-only protocol failure: never retain a payload or parser exception. */
public final class EventProtocolException extends RuntimeException {
    private final String code;

    public EventProtocolException(String code) {
        super(code);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
