-- version is the optimistic lock: a reservation reads a row, decides, and writes back only if
-- nobody changed it in between.
CREATE TABLE stock(
  sku TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  on_hand INT NOT NULL CHECK(on_hand >= 0),
  reserved INT NOT NULL DEFAULT 0 CHECK(reserved >= 0 AND reserved <= on_hand),
  version INT NOT NULL DEFAULT 1);
INSERT INTO stock(sku,name,on_hand) VALUES
  ('SKU-KEYBOARD', 'Mechanical keyboard', 25),
  ('SKU-MOUSE', 'Wireless mouse', 100),
  ('SKU-MONITOR', '27" monitor', 3),
  ('SKU-CABLE', 'USB-C cable', 500);

-- RELEASED without a prior reservation records a release that arrived first, so a late
-- reservation for a cancelled order is refused.
CREATE TABLE reservation(
  order_id UUID PRIMARY KEY,
  reservation_id UUID,
  status TEXT NOT NULL CHECK(status IN ('RESERVED','REJECTED','RELEASED')),
  lines JSONB NOT NULL,
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
