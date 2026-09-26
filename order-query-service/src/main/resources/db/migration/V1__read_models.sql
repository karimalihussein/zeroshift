-- Read models: denormalised for the questions they answer, derived only from order.events, and
-- disposable. A rebuild truncates them and replays the topic from offset 0.
CREATE TABLE order_view(
  order_id UUID PRIMARY KEY,
  -- A customer's UUID for orders placed since ADR 021; older events carried a free-text name.
  customer_id TEXT NOT NULL,
  customer_name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PLACED','PAID','RESERVED','SHIPPED','CANCELLED')),
  currency CHAR(3) NOT NULL,
  subtotal NUMERIC(12,2) NOT NULL,
  discount NUMERIC(12,2) NOT NULL,
  tax_rate NUMERIC(5,4) NOT NULL,
  tax NUMERIC(12,2) NOT NULL,
  total NUMERIC(12,2) NOT NULL,
  voucher_code TEXT,
  invoice_number TEXT,
  -- Derived from the order's events: issued with the order, paid with it, voided on cancellation.
  invoice_status TEXT CHECK (invoice_status IN ('ISSUED','PAID','VOIDED')),
  item_count INT NOT NULL,
  -- The order's lines exactly as sold (OrderLine snapshots).
  items JSONB NOT NULL,
  payment_id UUID,
  reservation_id UUID,
  tracking_number TEXT,
  carrier TEXT,
  cancel_reason TEXT,
  compensations TEXT[] NOT NULL DEFAULT '{}',
  events_applied INT NOT NULL,
  last_event_type TEXT NOT NULL,
  last_event_id UUID NOT NULL,
  last_offset TEXT NOT NULL,
  placed_at TIMESTAMPTZ NOT NULL,
  projected_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());

CREATE INDEX order_view_placed_at ON order_view (placed_at DESC);
CREATE INDEX order_view_customer_placed_at ON order_view (customer_id, placed_at DESC);

-- The same events, shaped for a different question: what has each customer bought?
CREATE TABLE customer_summary(
  customer_id TEXT PRIMARY KEY,
  customer_name TEXT NOT NULL,
  currency CHAR(3) NOT NULL,
  orders_placed INT NOT NULL DEFAULT 0,
  orders_shipped INT NOT NULL DEFAULT 0,
  orders_cancelled INT NOT NULL DEFAULT 0,
  shipped_value NUMERIC(12,2) NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
