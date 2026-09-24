-- Rollback after cutover: PostgreSQL changes are captured durably in the same transaction as the
-- write, replayed into SQL Server, and acknowledged only after SQL Server has committed them.

-- One row per captured write. seq is per-key commit ordered: a later write to the same key waits
-- on the earlier one's row lock, so it always receives a higher seq.
CREATE TABLE IF NOT EXISTS reverse_change(
 seq BIGSERIAL PRIMARY KEY, table_name TEXT NOT NULL, record_id BIGINT NOT NULL,
 operation CHAR(1) NOT NULL, captured_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS reverse_change_key ON reverse_change(table_name, record_id, seq);

-- SQL Server rows changed outside ZeroShift after cutover. Rollback refuses to overwrite them.
CREATE TABLE IF NOT EXISTS reverse_conflict(
 table_name TEXT NOT NULL, record_id BIGINT NOT NULL, detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(table_name, record_id));

-- PostgreSQL write fence, mirroring SQL Server's migration_gate.
CREATE TABLE IF NOT EXISTS write_gate(id INT PRIMARY KEY CHECK(id=1), frozen BOOLEAN NOT NULL DEFAULT FALSE);
INSERT INTO write_gate(id) VALUES(1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS rollback_state(
 id INT PRIMARY KEY CHECK(id=1), capture_since TIMESTAMPTZ, baseline_version BIGINT NOT NULL DEFAULT 0,
 started_at TIMESTAMPTZ, freeze_at TIMESTAMPTZ, completed_at TIMESTAMPTZ,
 applied BIGINT NOT NULL DEFAULT 0, conflicts BIGINT NOT NULL DEFAULT 0,
 validation TEXT NOT NULL DEFAULT 'Not checked', validation_passed BOOLEAN NOT NULL DEFAULT FALSE,
 verified_rows BIGINT NOT NULL DEFAULT 0);
INSERT INTO rollback_state(id) VALUES(1) ON CONFLICT DO NOTHING;

-- Attached to customers/orders only while PostgreSQL is primary (see PostgresMigrationStore).
CREATE OR REPLACE FUNCTION zeroshift_capture() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('UPDATE','DELETE') THEN
    INSERT INTO reverse_change(table_name, record_id, operation) VALUES(upper(TG_TABLE_NAME), OLD.id, left(TG_OP,1));
  END IF;
  IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NEW.id <> OLD.id) THEN
    INSERT INTO reverse_change(table_name, record_id, operation) VALUES(upper(TG_TABLE_NAME), NEW.id, left(TG_OP,1));
  END IF;
  RETURN NULL;
END $$;

-- FOR SHARE makes the fence wait for in-flight writes: freezing blocks until they commit, and
-- writes that start after the freeze commits see it.
CREATE OR REPLACE FUNCTION zeroshift_fence() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE frozen BOOLEAN;
BEGIN
  SELECT g.frozen INTO frozen FROM write_gate g WHERE g.id=1 FOR SHARE;
  IF frozen THEN
    RAISE EXCEPTION 'PostgreSQL writes are frozen by ZeroShift' USING ERRCODE = '55000';
  END IF;
  RETURN NULL;
END $$;
