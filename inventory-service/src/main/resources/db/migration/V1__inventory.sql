-- The catalog and its stock, one row per product. stock is what can still be sold: reserving
-- takes from it with one conditional UPDATE (WHERE stock >= :q), releasing gives it back.
-- version counts every change.
CREATE TABLE product(
  id UUID PRIMARY KEY,
  sku TEXT NOT NULL UNIQUE CHECK(sku ~ '^[A-Z0-9][A-Z0-9-]{2,39}$'),
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  price NUMERIC(12,2) NOT NULL CHECK(price >= 0),
  stock INT NOT NULL CHECK(stock >= 0),
  version BIGINT NOT NULL DEFAULT 1,
  active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());

-- One per order. REJECTED has no items. RELEASED without items records a release that arrived
-- before the reservation, so a late reservation for a cancelled order is refused.
CREATE TABLE reservation(
  id UUID PRIMARY KEY,
  order_id UUID NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK(status IN ('RESERVED','REJECTED','RELEASED')),
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  released_at TIMESTAMPTZ);

CREATE TABLE reservation_item(
  reservation_id UUID NOT NULL REFERENCES reservation(id) ON DELETE CASCADE,
  product_id UUID NOT NULL REFERENCES product(id),
  quantity INT NOT NULL CHECK(quantity > 0),
  PRIMARY KEY(reservation_id, product_id));
