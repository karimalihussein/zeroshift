import { source, target } from './db.js';
import { TABLES } from './types.js';
import { currentRun, runtime } from './state.js';
import { dependencies, mappings } from './schema.js';
import { rollbackAssessment } from './cutover.js';
export async function status() {
  const run = await currentRun();
  const sourceCounts: Record<string, number> = {},
    targetCounts: Record<string, number> = {};
  for (const t of TABLES) {
    sourceCounts[t] = Number(
      (await source.request().query(`SELECT COUNT_BIG(*) n FROM ${t}`)).recordset[0].n,
    );
    targetCounts[t] = Number(
      (await target.query(`SELECT COUNT(*)::bigint n FROM "${t}"`)).rows[0].n,
    );
  }
  const maxChange = Number(
    (await source.request().query('SELECT ISNULL(MAX(change_id),0) n FROM migration_change_log'))
      .recordset[0].n,
  );
  const checkpoints = run
    ? (
        await target.query(
          "SELECT * FROM migration_checkpoints WHERE run_id=$1 ORDER BY CASE table_name WHEN 'customers' THEN 1 WHEN 'products' THEN 2 WHEN 'orders' THEN 3 WHEN 'order_items' THEN 4 ELSE 5 END",
          [run.id],
        )
      ).rows
    : [];
  const logs = (await target.query('SELECT * FROM lab_events ORDER BY id DESC LIMIT 100')).rows;
  const validation =
    (await target.query("SELECT value FROM lab_settings WHERE key='latest_validation'")).rows[0]
      ?.value ?? null;
  const primary =
    (await target.query("SELECT value#>>'{}' v FROM lab_settings WHERE key='active_database'"))
      .rows[0]?.v ?? 'sqlserver';
  const copied = checkpoints.reduce((n: number, c: any) => n + Number(c.rows_processed), 0),
    total = Object.values(sourceCounts).reduce((a, b) => a + b, 0);
  return {
    run,
    primary,
    sourceCounts,
    targetCounts,
    totals: {
      source: total,
      target: Object.values(targetCounts).reduce((a, b) => a + b, 0),
      copied,
      percentage: total ? Math.min(100, Math.round((copied / total) * 100)) : 0,
    },
    cdc: {
      mechanism: 'Educational Transactional Change Log',
      sourcePosition: maxChange,
      appliedPosition: Number(run?.last_applied_change ?? 0),
      pending: Math.max(0, maxChange - Number(run?.last_applied_change ?? 0)),
      paused: runtime.cdcPaused,
    },
    runtime,
    checkpoints,
    logs,
    validation,
    mappings,
    dependencies,
    rollback: await rollbackAssessment(),
  };
}
