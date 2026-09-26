-- API-edge idempotency. A client sends an Idempotency-Key with POST /orders; the first request
-- claims it in the same transaction that writes OrderPlaced, so the key exists exactly when the
-- order does. A retry finds the key and gets the original answer instead of a second order. A
-- concurrent retry blocks on the primary key until the first commits, then finds it.
-- request_hash guards against a key reused for a different order body. The answer's amounts are
-- the order's own, read back from it rather than copied here.
CREATE TABLE idempotency_key(
  key TEXT PRIMARY KEY,
  request_hash TEXT NOT NULL,
  order_id UUID NOT NULL REFERENCES orders(id),
  correlation_id UUID NOT NULL,
  event_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
