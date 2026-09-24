-- Row count captured at completion, so the summary survives later traffic on the target.
ALTER TABLE migration_state ADD COLUMN IF NOT EXISTS completed_rows BIGINT NOT NULL DEFAULT 0;
