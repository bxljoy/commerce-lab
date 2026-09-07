INSERT INTO stock (sku, available_quantity)
VALUES ('SKU-APPLE', 10), ('SKU-BANANA', 5)
ON CONFLICT DO NOTHING;
