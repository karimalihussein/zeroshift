import { randomUUID } from 'node:crypto';
import { target } from './db.js';
import type { MigrationState, Runtime, MigrationConfig } from './types.js';
const transitions: Record<MigrationState, MigrationState[]> = {
  IDLE: ['PREPARING'],
  PREPARING: ['CAPTURING_CHANGES', 'FAILED'],
  CAPTURING_CHANGES: ['INITIAL_COPY', 'FAILED'],
  INITIAL_COPY: ['CATCHING_UP', 'PAUSED', 'FAILED'],
  CATCHING_UP: ['BUILDING_INDEXES', 'PAUSED', 'FAILED'],
  BUILDING_INDEXES: ['SYNCING_SEQUENCES', 'FAILED'],
  SYNCING_SEQUENCES: ['ANALYZING', 'FAILED'],
  ANALYZING: ['VALIDATING', 'FAILED'],
  VALIDATING: ['READY_FOR_CUTOVER', 'FAILED'],
  READY_FOR_CUTOVER: ['FREEZING_WRITES', 'VALIDATING'],
  FREEZING_WRITES: ['FINAL_SYNC', 'FAILED'],
  FINAL_SYNC: ['CUTTING_OVER', 'FAILED'],
  CUTTING_OVER: ['MONITORING', 'FAILED'],
  MONITORING: ['COMPLETED'],
  COMPLETED: ['PREPARING'],
  FAILED: ['PREPARING'],
  PAUSED: ['INITIAL_COPY', 'CATCHING_UP', 'FAILED'],
};
export const isValidTransition = (from: MigrationState, to: MigrationState) =>
  transitions[from].includes(to);
export const runtime: Runtime = {
  trafficRunning: false,
  trafficRate: 5,
  trafficStats: { insert: 0, update: 0, delete: 0, errors: 0 },
  failNextBatch: false,
  networkDelay: 0,
  cdcPaused: false,
  workerAbort: false,
  rowsPerSecond: 0,
  lastBatchMs: 0,
};
export async function currentRun() {
  return (
    (await target.query('SELECT * FROM migration_runs ORDER BY updated_at DESC LIMIT 1')).rows[0] ??
    null
  );
}
export async function newRun(cfg: MigrationConfig) {
  const id = randomUUID();
  await target.query(
    "INSERT INTO migration_runs(id,state,config,started_at) VALUES($1,'IDLE',$2,now())",
    [id, JSON.stringify(cfg)],
  );
  return id;
}
export async function setState(id: string, next: MigrationState, force = false) {
  const r = await target.query('SELECT state FROM migration_runs WHERE id=$1', [id]);
  const current = (r.rows[0]?.state ?? 'IDLE') as MigrationState;
  if (!force && !isValidTransition(current, next))
    throw new Error(`Invalid migration transition: ${current} → ${next}`);
  if (next === 'PAUSED') runtime.previousState = current;
  await target.query('UPDATE migration_runs SET state=$2,updated_at=now() WHERE id=$1', [id, next]);
  await event('MIGRATION', 'INFO', `${current} → ${next}`, { state: next });
}
export async function event(
  category: string,
  level: string,
  message: string,
  details: unknown = {},
) {
  await target.query('INSERT INTO lab_events(category,level,message,details) VALUES($1,$2,$3,$4)', [
    category,
    level,
    message,
    JSON.stringify(details),
  ]);
}
export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
