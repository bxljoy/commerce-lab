-- A List in the domain/API has observable order, so persist that order explicitly.
-- Rows written before this migration have no recoverable original position. Assign a
-- deterministic position by UUID so existing orders remain readable and stable.

ALTER TABLE order_lines ADD COLUMN line_position INTEGER;

WITH ranked_lines AS (
    SELECT id,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY id) - 1 AS position
    FROM order_lines
)
UPDATE order_lines
SET line_position = ranked_lines.position
FROM ranked_lines
WHERE order_lines.id = ranked_lines.id;

ALTER TABLE order_lines ALTER COLUMN line_position SET NOT NULL;
ALTER TABLE order_lines ADD CONSTRAINT chk_order_lines_position_non_negative
    CHECK (line_position >= 0);
ALTER TABLE order_lines ADD CONSTRAINT uq_order_lines_order_position
    UNIQUE (order_id, line_position);
