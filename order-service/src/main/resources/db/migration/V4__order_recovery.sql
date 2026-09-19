ALTER TABLE orders
    ADD COLUMN version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN next_attempt_at TIMESTAMPTZ,
    ADD COLUMN last_attempt_at TIMESTAMPTZ,
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    ADD COLUMN recovery_blocked BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN last_failure_code VARCHAR(64),
    ADD COLUMN rejection_reason VARCHAR(64);

UPDATE orders
SET next_attempt_at = placed_at + INTERVAL '5 seconds'
WHERE status = 'PENDING_INVENTORY';

CREATE INDEX idx_orders_due_inventory ON orders (next_attempt_at, id)
    WHERE status = 'PENDING_INVENTORY' AND recovery_blocked = FALSE;

-- The originating correlation ID remains owned by order_requests (V3).
