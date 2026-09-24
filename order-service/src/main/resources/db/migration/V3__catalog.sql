-- Prices the order service charges. Stock levels belong to the inventory service.
CREATE TABLE product(sku TEXT PRIMARY KEY, name TEXT NOT NULL, price NUMERIC(12,2) NOT NULL CHECK(price >= 0));
INSERT INTO product VALUES
  ('SKU-KEYBOARD', 'Mechanical keyboard', 89.00),
  ('SKU-MOUSE', 'Wireless mouse', 29.50),
  ('SKU-MONITOR', '27" monitor', 329.00),
  ('SKU-CABLE', 'USB-C cable', 9.99);
