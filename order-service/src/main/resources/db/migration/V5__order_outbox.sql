DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM orders WHERE status = 'PENDING_INVENTORY') THEN
    RAISE EXCEPTION 'Phase 4A requires resolving all Phase 3B pending orders before migration';
  END IF;
END $$;
CREATE TABLE order_outbox (
 event_id UUID PRIMARY KEY,
 order_id UUID NOT NULL REFERENCES orders(id),
 event_type TEXT NOT NULL CHECK (event_type = 'OrderPlaced'),
 schema_version INTEGER NOT NULL CHECK (schema_version = 1),
 topic TEXT NOT NULL CHECK (topic = 'commerce.orders.v1'),
 message_key TEXT NOT NULL CHECK (message_key = order_id::text),
 payload TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 attempt_count BIGINT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
 next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 lease_token UUID,
 lease_until TIMESTAMPTZ,
 last_error_code VARCHAR(64),
 delivered_at TIMESTAMPTZ,
 UNIQUE (order_id, event_type),
 CHECK ((lease_token IS NULL) = (lease_until IS NULL)),
 CHECK ((jsonb_typeof(payload::jsonb) = 'object'
   AND payload::jsonb->>'eventId' = event_id::text
   AND payload::jsonb->>'orderId' = order_id::text
   AND payload::jsonb->>'eventType' = event_type
   AND payload::jsonb->'schemaVersion' = to_jsonb(schema_version)) IS TRUE)
);
CREATE INDEX ix_order_outbox_due
 ON order_outbox(next_attempt_at, created_at, event_id)
 WHERE delivered_at IS NULL;
