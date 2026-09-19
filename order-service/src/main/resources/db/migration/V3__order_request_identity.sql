ALTER TABLE orders ALTER COLUMN status TYPE VARCHAR(32);

CREATE TABLE order_requests (
    request_key VARCHAR(128) CONSTRAINT pk_order_requests_key PRIMARY KEY,
    order_id UUID NOT NULL CONSTRAINT uq_order_requests_order_id UNIQUE
        REFERENCES orders(id),
    canonical_payload JSONB NOT NULL,
    fingerprint VARCHAR(64) NOT NULL,
    fingerprint_version INTEGER NOT NULL CHECK (fingerprint_version = 1),
    correlation_id VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
