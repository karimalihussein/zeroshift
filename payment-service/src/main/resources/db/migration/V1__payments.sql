-- One row per order. VOID records a refund that arrived before any authorization, so a late
-- authorization is refused instead of charging a cancelled order.
CREATE TABLE payment(
  order_id UUID PRIMARY KEY,
  payment_id UUID,
  status TEXT NOT NULL CHECK(status IN ('AUTHORIZED','DECLINED','REFUNDED','VOID')),
  amount NUMERIC(12,2) NOT NULL,
  currency TEXT NOT NULL,
  gateway_reference TEXT,
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());

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
