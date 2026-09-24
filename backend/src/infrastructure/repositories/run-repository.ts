import { randomUUID } from 'node:crypto';
import type pg from 'pg';
import type { MigrationRunRepository } from '../../application/ports.js';
import { MigrationStateMachine } from '../../domain/migration-state-machine.js';
import { TABLE_NAMES } from '../../domain/models.js';
import type {
  Checkpoint,
  MigrationConfig,
  MigrationRun,
  MigrationState,
  PrimaryDatabase,
  ResumableStage,
  TableName,
} from '../../domain/models.js';
import type { EventRepository } from '../../application/ports.js';

interface RunRow {
  id: string;
  state: MigrationState;
  snapshot_boundary: string | number;
  last_applied_change: string | number;
  active_database: PrimaryDatabase;
  writes_frozen: boolean;
  config: MigrationConfig;
  resume_stage: ResumableStage | null;
}

interface CheckpointRow {
  run_id: string;
  table_name: TableName;
  last_processed_id: string | number;
  rows_processed: string | number;
  batch_number: number;
  status: Checkpoint['status'];
  updated_at: Date;
}

const mapRun = (row: RunRow): MigrationRun => ({
  id: row.id,
  state: row.state,
  snapshotBoundary: Number(row.snapshot_boundary),
  lastAppliedChange: Number(row.last_applied_change),
  activeDatabase: row.active_database,
  writesFrozen: row.writes_frozen,
  config: row.config,
  resumeStage: row.resume_stage,
});

const mapCheckpoint = (row: CheckpointRow): Checkpoint => ({
  runId: row.run_id,
  tableName: row.table_name,
  lastProcessedId: Number(row.last_processed_id),
  rowsProcessed: Number(row.rows_processed),
  batchNumber: row.batch_number,
  status: row.status,
  updatedAt: row.updated_at,
});

export class PostgresMigrationRunRepository implements MigrationRunRepository {
  constructor(
    private readonly pool: pg.Pool,
    private readonly events: EventRepository,
    private readonly stateMachine: MigrationStateMachine,
  ) {}

  async current(): Promise<MigrationRun | null> {
    const result = await this.pool.query<RunRow>(
      'SELECT * FROM migration_runs ORDER BY updated_at DESC LIMIT 1',
    );
    return result.rows[0] ? mapRun(result.rows[0]) : null;
  }

  async create(config: MigrationConfig): Promise<MigrationRun> {
    const id = randomUUID();
    const result = await this.pool.query<RunRow>(
      "INSERT INTO migration_runs(id,state,config,started_at) VALUES($1,'IDLE',$2,now()) RETURNING *",
      [id, JSON.stringify(config)],
    );
    return mapRun(result.rows[0]!);
  }

  async transition(runId: string, state: MigrationState, force = false): Promise<void> {
    const run = await this.requiredRun(runId);
    if (!force) this.stateMachine.assertTransition(run.state, state);
    await this.pool.query('UPDATE migration_runs SET state=$2,updated_at=now() WHERE id=$1', [runId, state]);
    await this.events.append('MIGRATION', 'INFO', `${run.state} → ${state}`, { state });
  }

  async establishBoundary(runId: string, boundary: number): Promise<void> {
    await this.pool.query(
      'UPDATE migration_runs SET snapshot_boundary=$2,last_applied_change=$2 WHERE id=$1',
      [runId, boundary],
    );
  }

  async advanceChangePosition(runId: string, changeId: number): Promise<void> {
    await this.pool.query(
      'UPDATE migration_runs SET last_applied_change=GREATEST(last_applied_change,$2),updated_at=now() WHERE id=$1',
      [runId, changeId],
    );
  }

  async initializeCheckpoints(runId: string): Promise<void> {
    await this.pool.query('DELETE FROM migration_checkpoints WHERE run_id=$1', [runId]);
    for (const table of TABLE_NAMES) {
      await this.pool.query(
        "INSERT INTO migration_checkpoints(run_id,table_name,last_processed_id,status) VALUES($1,$2,-1,'pending')",
        [runId, table],
      );
    }
  }

  async checkpoint(runId: string, table: TableName): Promise<Checkpoint> {
    const result = await this.pool.query<CheckpointRow>(
      'SELECT * FROM migration_checkpoints WHERE run_id=$1 AND table_name=$2',
      [runId, table],
    );
    if (!result.rows[0]) throw new Error(`Checkpoint not found for ${table}`);
    return mapCheckpoint(result.rows[0]);
  }

  async checkpoints(runId: string): Promise<Checkpoint[]> {
    const result = await this.pool.query<CheckpointRow>(
      'SELECT * FROM migration_checkpoints WHERE run_id=$1 ORDER BY updated_at',
      [runId],
    );
    return result.rows.map(mapCheckpoint);
  }

  async completeTable(runId: string, table: TableName): Promise<void> {
    await this.pool.query(
      "UPDATE migration_checkpoints SET status='complete',updated_at=now() WHERE run_id=$1 AND table_name=$2",
      [runId, table],
    );
  }

  async determineResumeStage(runId: string): Promise<ResumableStage> {
    const run = await this.requiredRun(runId);
    if (run.resumeStage) return run.resumeStage;
    const result = await this.pool.query<{ count: number }>(
      "SELECT count(*)::int count FROM migration_checkpoints WHERE run_id=$1 AND status='complete'",
      [runId],
    );
    return result.rows[0]!.count === TABLE_NAMES.length ? 'CATCHING_UP' : 'INITIAL_COPY';
  }

  async setResumeStage(runId: string, stage: ResumableStage): Promise<void> {
    await this.pool.query('UPDATE migration_runs SET resume_stage=$2 WHERE id=$1', [runId, stage]);
  }

  async setPrimary(runId: string, database: PrimaryDatabase): Promise<void> {
    await this.pool.query(
      'UPDATE migration_runs SET active_database=$2,writes_frozen=false WHERE id=$1',
      [runId, database],
    );
  }

  private async requiredRun(runId: string): Promise<MigrationRun> {
    const result = await this.pool.query<RunRow>('SELECT * FROM migration_runs WHERE id=$1', [runId]);
    if (!result.rows[0]) throw new Error(`Migration run ${runId} not found`);
    return mapRun(result.rows[0]);
  }
}
