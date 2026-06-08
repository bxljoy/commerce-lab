-- Phase 2 — initial schema for the order aggregate.
-- Flyway owns the schema; Hibernate runs with ddl-auto=validate against it.

CREATE TABLE orders (
    id          UUID          PRIMARY KEY,
    customer_id VARCHAR(64)   NOT NULL,
    status      VARCHAR(16)   NOT NULL,
    currency    VARCHAR(3)    NOT NULL,
    placed_at   TIMESTAMPTZ   NOT NULL
);

CREATE TABLE order_lines (
    id         UUID            PRIMARY KEY,
    order_id   UUID            NOT NULL REFERENCES orders (id),
    sku        VARCHAR(64)     NOT NULL,
    quantity   INTEGER         NOT NULL,
    unit_price NUMERIC(19, 4)  NOT NULL
);

CREATE INDEX idx_order_lines_order_id ON order_lines (order_id);
