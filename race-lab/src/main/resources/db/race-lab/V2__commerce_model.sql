-- The race lab's commerce tables take the shapes of ZeroShift's one commerce model (ADR 021):
-- product (catalog and stock) as inventory-service has it, orders and payment (with its unique
-- idempotency key) as order- and payment-service have them. Each run still seeds its own rows,
-- tagged with run_id. The generic tables (account, deposit, document, doctor) stay as they are.

DROP TABLE IF EXISTS reservation, item, charge, payment, order_effect, shop_order;

CREATE TABLE product(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id BIGINT NOT NULL,
  sku TEXT NOT NULL,
  name TEXT NOT NULL,
  price NUMERIC(12,2) NOT NULL CHECK (price >= 0),
  stock INT NOT NULL CHECK (stock >= 0),
  version BIGINT NOT NULL DEFAULT 1,
  active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (run_id, sku));

-- One row per unit a request reserved (overselling, the lock queue).
CREATE TABLE reservation(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id BIGINT NOT NULL,
  product_id UUID NOT NULL REFERENCES product(id),
  order_id UUID NOT NULL,
  customer_id UUID NOT NULL,
  request_id TEXT NOT NULL,
  quantity INT NOT NULL DEFAULT 1 CHECK (quantity > 0),
  reserved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE INDEX reservation_product ON reservation(product_id);

CREATE TABLE orders(
  id UUID PRIMARY KEY,
  run_id BIGINT NOT NULL,
  customer_id UUID NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PLACED','PAID','RESERVED','SHIPPED','CANCELLED')),
  currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  total NUMERIC(12,2) NOT NULL CHECK (total >= 0),
  version BIGINT NOT NULL DEFAULT 1,
  placed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());

-- A charge attempt. The unique key is what makes a duplicate request harmless.
CREATE TABLE payment(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id BIGINT NOT NULL,
  order_id UUID NOT NULL REFERENCES orders(id),
  idempotency_key TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK (status IN ('PENDING','AUTHORIZED','DECLINED','REFUNDED','VOIDED')),
  method TEXT NOT NULL DEFAULT 'CARD' CHECK (method IN ('CARD')),
  amount NUMERIC(12,2) NOT NULL CHECK (amount > 0),
  currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  request_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  authorized_at TIMESTAMPTZ);
CREATE INDEX payment_order ON payment(order_id);

-- What a state transition did besides the status (a shipment or a refund): the lab's evidence.
CREATE TABLE order_effect(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id BIGINT NOT NULL,
  order_id UUID NOT NULL REFERENCES orders(id),
  effect TEXT NOT NULL CHECK (effect IN ('SHIPMENT','REFUND')),
  request_id TEXT NOT NULL,
  made_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE INDEX order_effect_order ON order_effect(order_id);
