-- The race condition lab's own schema (race_lab), with its own Flyway history. Nothing outside
-- this schema is read or written by the lab, and a reset truncates only these tables.

-- Runs and every event they recorded: enough evidence to inspect or compare a run afterwards.
CREATE TABLE run(
  id BIGSERIAL PRIMARY KEY,
  experiment TEXT NOT NULL,
  mode TEXT NOT NULL,
  isolation TEXT NOT NULL,
  status TEXT NOT NULL,
  config JSONB NOT NULL,
  requests JSONB NOT NULL,
  result JSONB,
  error TEXT,
  trace_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ);
CREATE INDEX run_experiment ON run(experiment, id DESC);

CREATE TABLE run_event(
  run_id BIGINT NOT NULL REFERENCES run(id) ON DELETE CASCADE,
  seq INT NOT NULL,
  type TEXT NOT NULL,
  lane TEXT NOT NULL,
  at_micros BIGINT NOT NULL,
  payload JSONB NOT NULL,
  PRIMARY KEY (run_id, seq));

-- Scenario rows. Each run seeds its own, tagged with run_id, so runs never share data and a
-- finished run's rows stay as its requests left them.

-- 1 Overselling · 6 row lock queue
CREATE TABLE item(
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL,
  sku TEXT NOT NULL,
  stock INT NOT NULL,
  version BIGINT NOT NULL);
CREATE TABLE reservation(
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL,
  item_id BIGINT NOT NULL,
  request_id TEXT NOT NULL,
  order_id TEXT NOT NULL,
  customer_id TEXT NOT NULL,
  reserved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE INDEX reservation_item ON reservation(item_id);

-- 2 Lost update · 7 deadlock · 8 non-repeatable read, phantom read
CREATE TABLE account(
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL,
  name TEXT NOT NULL,
  balance BIGINT NOT NULL,
  version BIGINT NOT NULL);
CREATE TABLE deposit(
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  request_id TEXT NOT NULL,
  amount BIGINT NOT NULL,
  made_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE INDEX deposit_account ON deposit(account_id);

-- 3 Double payment
CREATE TABLE payment(
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL,
  order_id TEXT NOT NULL,
  amount BIGINT NOT NULL,
  status TEXT NOT NULL,
  version BIGINT NOT NULL);
CREATE TABLE charge(
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL,
  payment_id BIGINT NOT NULL,
  request_id TEXT NOT NULL,
  amount BIGINT NOT NULL,
  charged_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE INDEX charge_payment ON charge(payment_id);

-- 4 Concurrent state transition
CREATE TABLE shop_order(
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL,
  order_id TEXT NOT NULL,
  status TEXT NOT NULL,
  version BIGINT NOT NULL);
CREATE TABLE order_effect(
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL,
  shop_order_id BIGINT NOT NULL,
  effect TEXT NOT NULL,
  request_id TEXT NOT NULL,
  made_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE INDEX order_effect_order ON order_effect(shop_order_id);

-- 5 Optimistic locking (stale edit)
CREATE TABLE document(
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL,
  title TEXT NOT NULL,
  version BIGINT NOT NULL);
CREATE TABLE document_revision(
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL,
  document_id BIGINT NOT NULL,
  based_on BIGINT NOT NULL,
  version BIGINT NOT NULL,
  title TEXT NOT NULL,
  request_id TEXT NOT NULL,
  saved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE INDEX document_revision_document ON document_revision(document_id);

-- 8 Write skew
CREATE TABLE doctor(
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL,
  name TEXT NOT NULL,
  shift TEXT NOT NULL,
  on_call BOOLEAN NOT NULL,
  version BIGINT NOT NULL);
CREATE INDEX doctor_shift ON doctor(run_id, shift);
