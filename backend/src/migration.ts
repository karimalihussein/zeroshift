import { Readable } from 'node:stream';
import { finished } from 'node:stream/promises';
import { from as copyFrom } from 'pg-copy-streams';
import sql from 'mssql';
import { source, target } from './db.js';
import { TABLES, type TableName, type MigrationConfig } from './types.js';
import { columns } from './schema.js';
import { currentRun, event, newRun, runtime, setState, sleep } from './state.js';
import { startTraffic, stopTraffic } from './traffic.js';
const q = (s: string) => `"${s}"`;
const delayFor = (speed: MigrationConfig['speed']) =>
  speed === 'slow' ? 1200 : speed === 'normal' ? 250 : 0;
function csv(v: unknown) {
  if (v === null || v === undefined) return '\\N';
  if (v instanceof Date) return v.toISOString();
  if (Buffer.isBuffer(v)) return v.toString();
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  const s = String(v);
  return /[",\n\r]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
}
async function copyBatch(client: any, table: TableName, rows: any[]) {
  if (!rows.length) return;
  const cols = columns[table];
  const stream = client.query(
    copyFrom(
      `COPY ${q(table)} (${cols.map(q).join(',')}) FROM STDIN WITH (FORMAT csv, NULL '\\N')`,
    ),
  );
  Readable.from(rows.map((row) => cols.map((c) => csv(row[c])).join(',') + '\n')).pipe(stream);
  await finished(stream);
}
async function ensureRun(cfg: MigrationConfig) {
  const prior = await currentRun();
  if (prior && !['COMPLETED', 'FAILED'].includes(prior.state))
    throw new Error(`A migration is already ${prior.state}`);
  const id = await newRun(cfg);
  await setState(id, 'PREPARING');
  return id;
}
export async function startMigration(
  cfg: MigrationConfig = { batchSize: 5000, cdcBatchSize: 500, speed: 'normal' },
) {
  const id = await ensureRun(cfg);
  runtime.workerAbort = false;
  void runMigration(id, cfg).catch(async (e) => {
    await event('ERROR', 'ERROR', e.message);
    await setState(id, 'FAILED', true);
  });
  return id;
}
export async function resumeMigration() {
  const run = await currentRun();
  if (!run || !['PAUSED', 'FAILED'].includes(run.state))
    throw new Error('No paused or failed migration to resume');
  runtime.workerAbort = false;
  const cfg = run.config as MigrationConfig;
  const stage = (
    (runtime.previousState ??
    (
      await target.query(
        "SELECT count(*)::int n FROM migration_checkpoints WHERE run_id=$1 AND status='complete'",
        [run.id],
      )
    ).rows[0].n === TABLES.length)
      ? 'CATCHING_UP'
      : 'INITIAL_COPY'
  ) as any;
  await setState(run.id, stage, true);
  void continueFrom(run.id, cfg, stage).catch(async (e) => {
    await event('ERROR', 'ERROR', e.message);
    await setState(run.id, 'FAILED', true);
  });
  return run.id;
}
async function runMigration(id: string, cfg: MigrationConfig) {
  await target.query(
    'TRUNCATE payments,order_items,orders,products,customers,applied_changes RESTART IDENTITY CASCADE',
  );
  await target.query('DELETE FROM migration_checkpoints WHERE run_id=$1', [id]);
  await setState(id, 'CAPTURING_CHANGES');
  const boundary = Number(
    (
      await source
        .request()
        .query('SELECT ISNULL(MAX(change_id),0) boundary FROM migration_change_log')
    ).recordset[0].boundary,
  );
  await target.query(
    'UPDATE migration_runs SET snapshot_boundary=$2,last_applied_change=$2 WHERE id=$1',
    [id, boundary],
  );
  for (const table of TABLES)
    await target.query(
      "INSERT INTO migration_checkpoints(run_id,table_name,last_processed_id,status) VALUES($1,$2,-1,'pending')",
      [id, table],
    );
  await event('CDC', 'INFO', 'Snapshot boundary established', {
    changeId: boundary,
    mechanism: 'Educational Transactional Change Log',
  });
  await setState(id, 'INITIAL_COPY');
  await continueFrom(id, cfg, 'INITIAL_COPY');
}
async function continueFrom(
  id: string,
  cfg: MigrationConfig,
  stage: 'INITIAL_COPY' | 'CATCHING_UP',
) {
  if (stage === 'INITIAL_COPY') {
    for (const table of TABLES) {
      const cp = (
        await target.query(
          'SELECT * FROM migration_checkpoints WHERE run_id=$1 AND table_name=$2',
          [id, table],
        )
      ).rows[0];
      if (cp.status === 'complete') continue;
      let last = Number(cp.last_processed_id);
      while (true) {
        if (runtime.workerAbort) return;
        const started = Date.now();
        if (runtime.failNextBatch) {
          runtime.failNextBatch = false;
          throw new Error('Injected failure before batch commit');
        }
        const req = source.request();
        req.input('last', sql.BigInt, last);
        req.input('size', sql.Int, cfg.batchSize);
        const rows = (
          await req.query(`SELECT TOP (@size) * FROM ${table} WHERE id > @last ORDER BY id`)
        ).recordset;
        if (!rows.length) break;
        if (runtime.networkDelay) await sleep(runtime.networkDelay);
        const client = await target.connect();
        try {
          await client.query('BEGIN');
          await copyBatch(client, table, rows);
          last = Number(rows.at(-1).id);
          await client.query(
            "UPDATE migration_checkpoints SET last_processed_id=$3,rows_processed=rows_processed+$4,batch_number=batch_number+1,status='running',updated_at=now() WHERE run_id=$1 AND table_name=$2",
            [id, table, last, rows.length],
          );
          await client.query('COMMIT');
        } catch (e) {
          await client.query('ROLLBACK');
          throw e;
        } finally {
          client.release();
        }
        runtime.lastBatchMs = Date.now() - started;
        runtime.rowsPerSecond = Math.round(
          rows.length / Math.max(runtime.lastBatchMs / 1000, 0.001),
        );
        await event('MIGRATION', 'INFO', `${table} batch committed`, {
          lastId: last,
          rows: rows.length,
          rowsPerSecond: runtime.rowsPerSecond,
        });
        await sleep(delayFor(cfg.speed));
      }
      await target.query(
        "UPDATE migration_checkpoints SET status='complete',updated_at=now() WHERE run_id=$1 AND table_name=$2",
        [id, table],
      );
      await event('MIGRATION', 'INFO', `${table} initial copy complete`);
    }
    await setState(id, 'CATCHING_UP');
  }
  await catchUp(id, cfg);
  if (runtime.workerAbort) return;
  await finishPreparation(id);
}
export async function catchUp(id: string, cfg: MigrationConfig, drain = false) {
  while (true) {
    if (runtime.workerAbort) return;
    if (runtime.cdcPaused && !drain) {
      await sleep(300);
      continue;
    }
    const run = await target.query('SELECT last_applied_change FROM migration_runs WHERE id=$1', [
      id,
    ]);
    const last = Number(run.rows[0].last_applied_change);
    const req = source.request();
    req.input('last', sql.BigInt, last);
    req.input('size', sql.Int, cfg.cdcBatchSize);
    const changes = (
      await req.query(
        'SELECT TOP (@size) * FROM migration_change_log WHERE change_id > @last ORDER BY change_id',
      )
    ).recordset;
    if (!changes.length) break;
    for (const change of changes) await applyChange(id, change);
    if (!drain && runtime.trafficRunning && changes.length < cfg.cdcBatchSize) break;
    await sleep(delayFor(cfg.speed));
  }
}
async function applyChange(id: string, change: any) {
  const table = change.table_name as TableName;
  if (!TABLES.includes(table)) throw new Error(`Unknown table ${table}`);
  const client = await target.connect();
  try {
    await client.query('BEGIN');
    const seen = await client.query('SELECT 1 FROM applied_changes WHERE change_id=$1', [
      change.change_id,
    ]);
    if (!seen.rowCount) {
      if (change.operation === 'D')
        await client.query(`DELETE FROM ${q(table)} WHERE id=$1`, [change.record_id]);
      else {
        const row = JSON.parse(change.row_data);
        const cols = columns[table];
        const vals = cols.map((c) => row[c] ?? null);
        const updates = cols
          .filter((c) => c !== 'id')
          .map((c) => `${q(c)}=EXCLUDED.${q(c)}`)
          .join(',');
        await client.query(
          `INSERT INTO ${q(table)} (${cols.map(q)}) VALUES (${cols.map((_, i) => '$' + (i + 1))}) ON CONFLICT(id) DO UPDATE SET ${updates}`,
          vals,
        );
      }
      await client.query('INSERT INTO applied_changes(change_id) VALUES($1)', [change.change_id]);
    }
    await client.query(
      'UPDATE migration_runs SET last_applied_change=GREATEST(last_applied_change,$2),updated_at=now() WHERE id=$1',
      [id, change.change_id],
    );
    await client.query('COMMIT');
    await event(
      'CDC',
      'INFO',
      `${change.operation === 'I' ? 'INSERT' : change.operation === 'U' ? 'UPDATE' : 'DELETE'} ${table} ${change.record_id}`,
      {
        changeId: change.change_id,
        table,
        recordId: change.record_id,
        operation: change.operation,
      },
    );
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}
async function finishPreparation(id: string) {
  const resumeTraffic = runtime.trafficRunning,
    rate = runtime.trafficRate;
  if (resumeTraffic) {
    stopTraffic();
    await sleep(1000);
    await event(
      'MIGRATION',
      'INFO',
      'Traffic paused briefly for a consistent readiness validation',
    );
  }
  const cfg = (await currentRun()).config as MigrationConfig;
  await catchUp(id, cfg, true);
  await setState(id, 'BUILDING_INDEXES');
  await target.query(
    'CREATE INDEX IF NOT EXISTS ix_orders_customer ON orders(customer_id); CREATE INDEX IF NOT EXISTS ix_items_order ON order_items(order_id); CREATE INDEX IF NOT EXISTS ix_payments_order ON payments(order_id)',
  );
  await setState(id, 'SYNCING_SEQUENCES');
  await syncSequences();
  await setState(id, 'ANALYZING');
  for (const table of TABLES) {
    await target.query(`ANALYZE ${q(table)}`);
    await event('MIGRATION', 'INFO', `ANALYZE ${table} ✓`);
  }
  await setState(id, 'VALIDATING');
  const { runValidation } = await import('./validation.js');
  const result = await runValidation();
  if (!result.passed) throw new Error('Validation failed; inspect mismatches');
  await setState(id, 'READY_FOR_CUTOVER');
  if (resumeTraffic) startTraffic(rate);
}
export async function syncSequences() {
  for (const table of TABLES)
    await target.query(
      `SELECT setval(pg_get_serial_sequence('${table}','id'),COALESCE((SELECT MAX(id) FROM ${q(table)}),0)+1,false)`,
    );
  await event('MIGRATION', 'INFO', 'PostgreSQL identity sequences synchronized');
}
export async function pauseMigration() {
  const run = await currentRun();
  if (!run || !['INITIAL_COPY', 'CATCHING_UP'].includes(run.state))
    throw new Error('Migration is not pausable');
  runtime.workerAbort = true;
  await setState(run.id, 'PAUSED');
}
export async function crashMigration() {
  const run = await currentRun();
  if (!run || !['INITIAL_COPY', 'CATCHING_UP'].includes(run.state))
    throw new Error('Migration worker is not active');
  runtime.workerAbort = true;
  await setState(run.id, 'FAILED', true);
  await event('ERROR', 'ERROR', 'Migration worker crash simulated; committed checkpoint is safe');
}
