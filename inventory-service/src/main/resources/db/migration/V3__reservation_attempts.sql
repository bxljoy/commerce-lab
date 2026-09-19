CREATE TABLE inventory_reservation_attempts (
    order_id UUID PRIMARY KEY,
    canonical_payload JSONB NOT NULL CHECK (jsonb_typeof(canonical_payload) = 'array'),
    outcome VARCHAR(16) CHECK (outcome IN ('RESERVED', 'REJECTED')),
    unavailable_skus JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK ((outcome IS NULL AND unavailable_skus IS NULL)
        OR (outcome = 'RESERVED' AND unavailable_skus IS NULL)
        OR (outcome = 'REJECTED' AND unavailable_skus IS NOT NULL
            AND jsonb_typeof(unavailable_skus) = 'object'))
);

-- SQL collation can differ from Java ordering. Readers normalize this array
-- through ReservationPayload before comparing semantic SKU/quantity content.
INSERT INTO inventory_reservation_attempts (order_id, canonical_payload, outcome, created_at)
SELECT r.order_id,
       jsonb_agg(jsonb_build_object('sku', l.sku, 'quantity', l.quantity) ORDER BY l.sku),
       'RESERVED', r.created_at
FROM inventory_reservations r
JOIN inventory_reservation_lines l ON l.order_id = r.order_id
GROUP BY r.order_id, r.created_at;
