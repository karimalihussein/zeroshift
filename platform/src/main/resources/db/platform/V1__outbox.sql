-- Transactional outbox. A service inserts here in the same transaction as its state change;
-- Debezium reads committed inserts from the WAL and publishes each row to row.topic, keyed by
-- aggregate_id. Nothing polls this table.
CREATE TABLE outbox(
  id UUID PRIMARY KEY,                 -- the event id: idempotency key downstream
  topic TEXT NOT NULL,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,          -- Kafka key: one order, one partition, one order of events
  type TEXT NOT NULL,
  schema_version INT NOT NULL,
  correlation_id UUID,
  causation_id UUID,
  traceparent TEXT,                    -- W3C trace context, carried to Kafka as a header
  payload JSONB NOT NULL,              -- the full envelope
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE INDEX outbox_aggregate ON outbox(aggregate_id, created_at);

-- Inserts only: deleting old rows must never publish anything.
CREATE PUBLICATION outbox_inserts FOR TABLE outbox WITH (publish = 'insert');
