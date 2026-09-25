-- CDC replay can be paused by the operator, and every replayed row keeps a version receipt
-- so a retried or overlapping capture window never applies an older image over a newer one.
ALTER TABLE migration_state ADD COLUMN IF NOT EXISTS cdc_paused BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TABLE IF NOT EXISTS replay_receipt(table_name TEXT NOT NULL, record_id BIGINT NOT NULL, version BIGINT NOT NULL, PRIMARY KEY(table_name,record_id));
