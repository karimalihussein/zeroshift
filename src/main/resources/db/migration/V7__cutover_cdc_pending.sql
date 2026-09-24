-- CDC changes still unapplied when PostgreSQL became primary, measured behind the source fence.
-- NULL for migrations completed before this was recorded: the summary must not invent a value.
ALTER TABLE migration_state ADD COLUMN IF NOT EXISTS completed_cdc_pending BIGINT;
