CREATE TABLE inventory_event_inbox (
 consumer_name TEXT NOT NULL,
 event_id UUID NOT NULL,
 content JSONB NOT NULL,
 processed_at TIMESTAMPTZ NOT NULL,
 PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE inventory_result_outbox (
 event_id UUID PRIMARY KEY,
 order_id UUID NOT NULL UNIQUE REFERENCES inventory_reservation_attempts(order_id),
 causation_id UUID NOT NULL UNIQUE,
 event_type TEXT NOT NULL CHECK (event_type IN ('InventoryReserved', 'InventoryRejected')),
 schema_version INTEGER NOT NULL CHECK (schema_version = 1),
 topic TEXT NOT NULL CHECK (topic = 'commerce.inventory.v1'),
 message_key TEXT NOT NULL CHECK (message_key = order_id::text),
 payload TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 attempt_count BIGINT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
 next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 lease_token UUID,
 lease_until TIMESTAMPTZ,
 last_error_code VARCHAR(64),
 delivered_at TIMESTAMPTZ,
 CHECK ((lease_token IS NULL) = (lease_until IS NULL)),
 CHECK ((jsonb_typeof(payload::jsonb) = 'object'
   AND payload::jsonb->>'eventId' = event_id::text
   AND payload::jsonb->>'orderId' = order_id::text
   AND payload::jsonb->>'causationId' = causation_id::text
   AND payload::jsonb->>'eventType' = event_type
   AND payload::jsonb->'schemaVersion' = to_jsonb(schema_version)) IS TRUE)
);
CREATE INDEX ix_inventory_result_outbox_due
 ON inventory_result_outbox(next_attempt_at, created_at, event_id)
 WHERE delivered_at IS NULL;
