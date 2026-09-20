CREATE TABLE order_event_inbox (
  consumer_name TEXT NOT NULL,
  event_id UUID NOT NULL,
  content JSONB NOT NULL CHECK (jsonb_typeof(content)='object'),
  processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (consumer_name,event_id)
);

CREATE TABLE order_inventory_results (
  order_id UUID PRIMARY KEY REFERENCES orders(id),
  causation_id UUID NOT NULL UNIQUE REFERENCES order_outbox(event_id),
  result_event_id UUID NOT NULL UNIQUE,
  consumer_name TEXT NOT NULL DEFAULT 'order-inventory-result-v1'
    CHECK (consumer_name='order-inventory-result-v1'),
  result_type TEXT NOT NULL
    CHECK (result_type IN ('InventoryReserved','InventoryRejected')),
  FOREIGN KEY (consumer_name,result_event_id)
    REFERENCES order_event_inbox(consumer_name,event_id)
);
