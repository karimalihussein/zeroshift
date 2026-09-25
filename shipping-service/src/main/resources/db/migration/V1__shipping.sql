CREATE TABLE shipment(
  order_id UUID PRIMARY KEY,
  status TEXT NOT NULL CHECK(status IN ('SCHEDULED','FAILED')),
  tracking_number TEXT,
  carrier TEXT,
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
