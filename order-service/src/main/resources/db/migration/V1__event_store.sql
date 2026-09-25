-- Event store: the only durable record of what happened to an order. Rows are never updated or
-- deleted. UNIQUE(stream_id, version) is the optimistic lock: two writers that loaded the same
-- version cannot both append the next one.
CREATE TABLE event_store(
  global_position BIGSERIAL PRIMARY KEY,
  stream_id UUID NOT NULL,
  version BIGINT NOT NULL,
  event_id UUID NOT NULL UNIQUE,
  type TEXT NOT NULL,
  schema_version INT NOT NULL,
  envelope JSONB NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(stream_id, version));

-- A cached fold, never the truth: it can be deleted at any time and rebuilt from event_store.
-- A snapshot written by an older format is ignored rather than trusted.
CREATE TABLE order_snapshot(
  stream_id UUID PRIMARY KEY,
  version BIGINT NOT NULL,
  format INT NOT NULL,
  state JSONB NOT NULL,
  taken_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
