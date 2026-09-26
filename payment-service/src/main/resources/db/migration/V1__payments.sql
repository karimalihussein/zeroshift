-- One row per charge attempt, identified by its idempotency key: the same key never charges twice.
-- A key is claimed (PENDING) before the gateway is called, then AUTHORIZED or DECLINED; an authorized
-- payment can be REFUNDED. VOIDED records a refund that arrived before the order's authorization,
-- under the saga's key, so a late authorization is refused instead of charging a cancelled order:
-- such a row never had an amount, the only one allowed without.
CREATE TABLE payment(
  id UUID PRIMARY KEY,
  order_id UUID NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK (status IN ('PENDING','AUTHORIZED','DECLINED','REFUNDED','VOIDED')),
  method TEXT NOT NULL DEFAULT 'CARD' CHECK (method IN ('CARD')),
  amount NUMERIC(12,2) CHECK (amount > 0),
  currency CHAR(3) CHECK (currency ~ '^[A-Z]{3}$'),
  provider TEXT NOT NULL,
  provider_reference TEXT,
  failure_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  authorized_at TIMESTAMPTZ,
  declined_at TIMESTAMPTZ,
  refunded_at TIMESTAMPTZ,
  voided_at TIMESTAMPTZ,
  CHECK ((amount IS NULL) = (currency IS NULL)),
  CHECK (amount IS NOT NULL OR status = 'VOIDED'));
CREATE INDEX payment_order ON payment(order_id);

-- Every gateway attempt, including those the circuit breaker refused. Written in its own
-- transaction so a failed delivery still leaves its attempts visible.
CREATE TABLE gateway_call(
  id BIGSERIAL PRIMARY KEY,
  order_id UUID NOT NULL,
  outcome TEXT NOT NULL,
  latency_ms BIGINT NOT NULL,
  breaker_state TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '',
  at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE INDEX gateway_call_order ON gateway_call(order_id, id);
