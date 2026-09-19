-- Controlled committed-pending fixture while order-service is stopped.
-- This models the state before the first remote attempt, NOT an HTTP crash hook.
BEGIN;
INSERT INTO orders (id, customer_id, status, currency, placed_at, next_attempt_at)
VALUES (:'id', 'before-attempt-fixture', 'PENDING_INVENTORY', 'EUR', now(), now());
INSERT INTO order_lines (id, order_id, sku, quantity, unit_price, line_position)
VALUES (:'line_id', :'id', 'SKU-APPLE', 1, 2.5, 0);
INSERT INTO order_requests
    (request_key, order_id, canonical_payload, fingerprint, fingerprint_version, correlation_id)
VALUES (:'key', :'id', :'canonical'::jsonb, :'fingerprint', 1, 'before-attempt-fixture');
COMMIT;
