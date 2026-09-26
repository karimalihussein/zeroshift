-- The commerce model (ADR 021). Customers and vouchers are this service's own data. orders,
-- order_item and invoice are the relational copy of each order, written in the same transaction
-- as every event appended to event_store: the event store stays the source of truth, these tables
-- give invoices, vouchers and customers real foreign keys (and the lab a cache to compare with the
-- fold). Money is NUMERIC(12,2); every amount travels with its CHAR(3) currency.
CREATE TABLE customer(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  phone TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE UNIQUE INDEX customer_email ON customer(lower(email));

-- usage_count only moves through one conditional UPDATE (redeem) and its inverse (a cancelled
-- order gives its use back); the CHECK makes overselling a limited voucher impossible even for a
-- statement that forgets the condition.
CREATE TABLE voucher(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  code TEXT NOT NULL UNIQUE CHECK (code = upper(code)),
  discount_type TEXT NOT NULL CHECK (discount_type IN ('FIXED', 'PERCENTAGE')),
  value NUMERIC(12,2) NOT NULL CHECK (value > 0),
  minimum_amount NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (minimum_amount >= 0),
  maximum_discount NUMERIC(12,2) CHECK (maximum_discount > 0),
  usage_limit INT CHECK (usage_limit > 0),
  usage_count INT NOT NULL DEFAULT 0
    CHECK (usage_count >= 0 AND (usage_limit IS NULL OR usage_count <= usage_limit)),
  valid_from TIMESTAMPTZ NOT NULL,
  valid_until TIMESTAMPTZ,
  active BOOLEAN NOT NULL DEFAULT true,
  version BIGINT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (discount_type <> 'PERCENTAGE' OR value <= 100),
  CHECK (valid_until IS NULL OR valid_until > valid_from));

-- version is the event stream's version: the row is exactly as current as the events behind it.
-- The voucher columns are snapshots: editing or deactivating the voucher never changes the order.
CREATE TABLE orders(
  id UUID PRIMARY KEY,
  customer_id UUID NOT NULL REFERENCES customer(id),
  status TEXT NOT NULL CHECK (status IN ('PLACED', 'PAID', 'RESERVED', 'SHIPPED', 'CANCELLED')),
  currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  subtotal NUMERIC(12,2) NOT NULL CHECK (subtotal >= 0),
  discount NUMERIC(12,2) NOT NULL CHECK (discount >= 0 AND discount <= subtotal),
  tax_rate NUMERIC(5,4) NOT NULL CHECK (tax_rate >= 0 AND tax_rate < 1),
  tax NUMERIC(12,2) NOT NULL CHECK (tax >= 0),
  total NUMERIC(12,2) NOT NULL CHECK (total = subtotal - discount + tax),
  voucher_id UUID REFERENCES voucher(id),
  voucher_code TEXT,
  voucher_discount_type TEXT CHECK (voucher_discount_type IN ('FIXED', 'PERCENTAGE')),
  voucher_value NUMERIC(12,2),
  payment_id UUID,
  reservation_id UUID,
  tracking_number TEXT,
  cancel_reason TEXT,
  version BIGINT NOT NULL,
  placed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  paid_at TIMESTAMPTZ,
  shipped_at TIMESTAMPTZ,
  cancelled_at TIMESTAMPTZ);
CREATE INDEX orders_customer ON orders(customer_id, placed_at DESC);
CREATE INDEX orders_placed ON orders(placed_at DESC);
CREATE INDEX orders_status ON orders(status);

-- What was sold, as it was sold: product id, SKU, name and unit price are copied from the catalog
-- (inventory-service) at placement. product_id is a plain UUID: the catalog is another database.
CREATE TABLE order_item(
  id UUID PRIMARY KEY,
  order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  line_no INT NOT NULL,
  product_id UUID,
  sku TEXT NOT NULL,
  product_name TEXT NOT NULL,
  unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
  quantity INT NOT NULL CHECK (quantity BETWEEN 1 AND 999),
  subtotal NUMERIC(12,2) NOT NULL CHECK (subtotal = unit_price * quantity),
  discount NUMERIC(12,2) NOT NULL CHECK (discount >= 0 AND discount <= subtotal),
  total NUMERIC(12,2) NOT NULL CHECK (total = subtotal - discount),
  UNIQUE (order_id, line_no));

-- Numbers come from a sequence: unique and increasing, with gaps where a placement rolled back.
CREATE SEQUENCE invoice_number_seq;
CREATE TABLE invoice(
  id UUID PRIMARY KEY,
  order_id UUID NOT NULL UNIQUE REFERENCES orders(id),
  number TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK (status IN ('ISSUED', 'PAID', 'VOIDED')),
  currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  subtotal NUMERIC(12,2) NOT NULL CHECK (subtotal >= 0),
  discount NUMERIC(12,2) NOT NULL CHECK (discount >= 0 AND discount <= subtotal),
  tax NUMERIC(12,2) NOT NULL CHECK (tax >= 0),
  total NUMERIC(12,2) NOT NULL CHECK (total = subtotal - discount + tax),
  issued_at TIMESTAMPTZ NOT NULL,
  paid_at TIMESTAMPTZ,
  voided_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
