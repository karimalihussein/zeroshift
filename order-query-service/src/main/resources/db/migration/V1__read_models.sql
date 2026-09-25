-- Read models: denormalised for the questions they answer, derived only from order.events, and
-- disposable. A rebuild truncates them and replays the topic from offset 0.
CREATE TABLE order_view(
  order_id UUID PRIMARY KEY,
  customer_id TEXT NOT NULL,
  status TEXT NOT NULL,
  total NUMERIC(12,2) NOT NULL,
  currency TEXT NOT NULL,
  item_count INT NOT NULL,
  lines JSONB NOT NULL,
  payment_id UUID,
  reservation_id UUID,
  tracking_number TEXT,
  cancel_reason TEXT,
  compensations TEXT[] NOT NULL DEFAULT '{}',
  events_applied INT NOT NULL,
  last_event_type TEXT NOT NULL,
  last_event_id UUID NOT NULL,
  last_offset TEXT NOT NULL,
  placed_at TIMESTAMPTZ NOT NULL,
  projected_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());

-- The same events, shaped for a different question: what has each customer bought?
CREATE TABLE customer_summary(
  customer_id TEXT PRIMARY KEY,
  orders_placed INT NOT NULL DEFAULT 0,
  orders_shipped INT NOT NULL DEFAULT 0,
  orders_cancelled INT NOT NULL DEFAULT 0,
  shipped_value NUMERIC(14,2) NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
