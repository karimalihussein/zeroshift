import { target } from './db.js';
import { catchUp, syncSequences } from './migration.js';
import { runValidation } from './validation.js';
import { currentRun, event, setState, sleep } from './state.js';
import type { MigrationConfig } from './types.js';
export async function cutover() {
  const run = await currentRun();
  if (!run || run.state !== 'READY_FOR_CUTOVER')
    throw new Error(`Cutover requires READY_FOR_CUTOVER, current state is ${run?.state ?? 'none'}`);
  await setState(run.id, 'FREEZING_WRITES');
  await target.query("UPDATE lab_settings SET value='true' WHERE key='writes_frozen'");
  await event('MIGRATION', 'INFO', 'Application writes frozen');
  await sleep(300);
  await setState(run.id, 'FINAL_SYNC');
  await catchUp(run.id, run.config as MigrationConfig, true);
  const fresh = await currentRun();
  const max = Number(
    (
      await (
        await import('./db.js')
      ).source
        .request()
        .query('SELECT ISNULL(MAX(change_id),0) n FROM migration_change_log')
    ).recordset[0].n,
  );
  if (Number(fresh.last_applied_change) !== max)
    throw new Error('Final CDC drain did not reach the source position');
  const validation = await runValidation();
  if (!validation.passed) throw new Error('Final validation failed');
  await syncSequences();
  await setState(run.id, 'CUTTING_OVER');
  await target.query(
    "UPDATE lab_settings SET value='\"postgres\"' WHERE key='active_database'; UPDATE lab_settings SET value='false' WHERE key='writes_frozen'",
  );
  await target.query(
    "UPDATE migration_runs SET active_database='postgres',writes_frozen=false WHERE id=$1",
    [run.id],
  );
  await setState(run.id, 'MONITORING');
  await event('MIGRATION', 'INFO', 'Cutover complete; PostgreSQL is now primary');
  return { primary: 'postgres', validation };
}
export async function rollbackAssessment() {
  const active = (
    await target.query("SELECT value#>>'{}' v FROM lab_settings WHERE key='active_database'")
  ).rows[0].v;
  const writes = Number(
    (await target.query("SELECT value#>>'{}' v FROM lab_settings WHERE key='postgres_only_writes'"))
      .rows[0].v,
  );
  return {
    activeDatabase: active,
    postgresOnlyWrites: writes,
    safe: active === 'sqlserver' || writes === 0,
    explanation: writes
      ? 'Direct rollback would lose PostgreSQL-only writes. Reconciliation or reverse replication is required.'
      : 'No target-only writes are recorded; rollback remains straightforward.',
  };
}
