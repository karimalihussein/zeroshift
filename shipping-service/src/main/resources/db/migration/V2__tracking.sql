-- Parcel tracking, projected from the carrier's scan events (shipping.carrier-scans). Scans of one
-- parcel only arrive in order if they share a partition; this projection records what arrived
-- when, so an out-of-order arrival is visible instead of silently corrupting the status.
CREATE TABLE tracking(
  tracking_number TEXT PRIMARY KEY,
  order_id UUID NOT NULL,
  status TEXT NOT NULL,
  last_seq INT NOT NULL,
  scans_applied INT NOT NULL DEFAULT 0,
  regressions INT NOT NULL DEFAULT 0,   -- an older scan overwrote a newer status
  stale_skipped INT NOT NULL DEFAULT 0, -- an older scan was refused by the sequence guard
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());

-- Every scan as it was handled: where it came from on Kafka and what it did to the projection.
CREATE TABLE tracking_scan(
  id BIGSERIAL PRIMARY KEY,
  tracking_number TEXT NOT NULL,
  seq INT NOT NULL,
  status TEXT NOT NULL,
  record_key TEXT,
  kafka_partition INT NOT NULL,
  kafka_offset BIGINT NOT NULL,
  outcome TEXT NOT NULL CHECK(outcome IN ('APPLIED', 'REGRESSED', 'STALE_SKIPPED')),
  at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE INDEX tracking_scan_parcel ON tracking_scan(tracking_number, id);

-- Operator switches for the lab that are settings, not faults: the sequence guard.
CREATE TABLE lab_setting(
  name TEXT PRIMARY KEY,
  value TEXT NOT NULL);
