-- Idempotent consumer: a message's effects and its row here commit together, so a redelivered
-- message (Kafka is at-least-once) finds its row and is skipped.
CREATE TABLE processed_message(
  consumer TEXT NOT NULL,
  event_id UUID NOT NULL,
  topic TEXT NOT NULL,
  kafka_partition INT NOT NULL,
  kafka_offset BIGINT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(consumer, event_id));

-- Every delivery outcome, including retries and dead letters, for the control plane.
CREATE TABLE consumer_decision(
  id BIGSERIAL PRIMARY KEY,
  consumer TEXT NOT NULL,
  event_id UUID,
  order_id TEXT,
  type TEXT,
  topic TEXT NOT NULL,
  kafka_partition INT NOT NULL,
  kafka_offset BIGINT NOT NULL,
  decision TEXT NOT NULL,
  attempt INT NOT NULL DEFAULT 1,
  detail TEXT NOT NULL DEFAULT '',
  trace_id TEXT,
  at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE INDEX consumer_decision_order ON consumer_decision(order_id, id);

-- Operator-armed failures. Durable, so a fault survives the crash it may cause.
CREATE TABLE lab_fault(
  name TEXT PRIMARY KEY,
  mode TEXT NOT NULL,
  remaining INT,                       -- NULL: until cleared
  armed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
