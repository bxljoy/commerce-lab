CREATE TABLE stock (
    sku VARCHAR(64) PRIMARY KEY,
    available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0)
);

CREATE TABLE inventory_reservations (
    order_id UUID PRIMARY KEY,
    status VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    released_at TIMESTAMPTZ
);

CREATE TABLE inventory_reservation_lines (
    order_id UUID NOT NULL REFERENCES inventory_reservations(order_id),
    sku VARCHAR(64) NOT NULL REFERENCES stock(sku),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    line_position INTEGER NOT NULL CHECK (line_position >= 0),
    PRIMARY KEY (order_id, sku),
    UNIQUE (order_id, line_position)
);
