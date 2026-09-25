-- Control plane for the event-driven lab. The tap is the control plane's own Kafka consumer
-- group: it copies every lab record (topic, partition, offset, headers, value) here, so the
-- dashboard can show where each message landed without re-reading Kafka.
CREATE TABLE IF NOT EXISTS event_tap(
  topic TEXT NOT NULL,
  kafka_partition INT NOT NULL,
  kafka_offset BIGINT NOT NULL,
  record_key TEXT,
  event_id UUID,
  type TEXT,
  headers JSONB NOT NULL,
  value TEXT,
  kafka_timestamp TIMESTAMPTZ NOT NULL,
  tapped_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(topic, kafka_partition, kafka_offset));
CREATE INDEX IF NOT EXISTS event_tap_key ON event_tap(record_key, kafka_timestamp);
CREATE INDEX IF NOT EXISTS event_tap_event ON event_tap(event_id);

-- Every lever an operator pulled, and what the target answered.
CREATE TABLE IF NOT EXISTS operator_action(
  id BIGSERIAL PRIMARY KEY,
  action TEXT NOT NULL,
  target TEXT NOT NULL,
  outcome TEXT NOT NULL,
  ok BOOLEAN NOT NULL,
  at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
