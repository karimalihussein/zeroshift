ALTER TABLE migration_state
  ADD COLUMN IF NOT EXISTS validation_passed BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS cutover_started_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;

-- Preserve durable state created by releases that used the less explicit terminal names.
UPDATE migration_state SET stage='COMPLETED' WHERE stage='COMPLETE';
UPDATE migration_state SET status='SUCCESS' WHERE status='COMPLETE';
UPDATE migration_state
SET validation_passed=TRUE
WHERE validation LIKE 'Passed:%' OR stage='COMPLETED';
UPDATE migration_state SET started_at=checkpoint WHERE stage<>'IDLE' AND started_at IS NULL;
UPDATE migration_state
SET cutover_started_at=checkpoint
WHERE stage IN ('FREEZE','VALIDATION','CUTOVER','COMPLETED') AND cutover_started_at IS NULL;
UPDATE migration_state SET completed_at=checkpoint WHERE stage='COMPLETED' AND completed_at IS NULL;
