import type {
  ChangeEvent,
  Checkpoint,
  DatabaseRow,
  MigrationConfig,
  MigrationRun,
  MigrationState,
  PrimaryDatabase,
  ResumableStage,
  TableName,
  ValidationResult,
} from '../domain/models.js';

export interface SourceDatabase {
  initialize(): Promise<void>;
  close(): Promise<void>;
  snapshotBoundary(): Promise<number>;
  readBatch(table: TableName, afterId: number, size: number): Promise<DatabaseRow[]>;
  readChanges(afterId: number, size: number): Promise<ChangeEvent[]>;
  count(table: TableName): Promise<number>;
  idRange(table: TableName): Promise<{ min: number | null; max: number | null }>;
  readRows(table: TableName, ids: readonly number[]): Promise<DatabaseRow[]>;
  generateData(customers: number, orders: number): Promise<Record<string, number>>;
  performTrafficOperation(): Promise<'insert' | 'update' | 'delete' | null>;
  reset(): Promise<void>;
}

export interface TargetDatabase {
  initialize(): Promise<void>;
  close(): Promise<void>;
  resetData(): Promise<void>;
  copyBatchAndCheckpoint(runId: string, table: TableName, rows: DatabaseRow[]): Promise<void>;
  applyChange(runId: string, change: ChangeEvent): Promise<boolean>;
  buildIndexes(): Promise<void>;
  synchronizeSequences(): Promise<void>;
  analyze(table: TableName): Promise<void>;
  count(table: TableName): Promise<number>;
  idRange(table: TableName): Promise<{ min: number | null; max: number | null }>;
  readRows(table: TableName, ids: readonly number[]): Promise<DatabaseRow[]>;
  relationshipsAreValid(): Promise<boolean>;
  corruptDemoRecord(): Promise<{ id: number }>;
  performTrafficOperation(): Promise<'insert' | 'update' | null>;
  resetLab(): Promise<void>;
}

export interface MigrationRunRepository {
  current(): Promise<MigrationRun | null>;
  create(config: MigrationConfig): Promise<MigrationRun>;
  transition(runId: string, state: MigrationState, force?: boolean): Promise<void>;
  establishBoundary(runId: string, boundary: number): Promise<void>;
  advanceChangePosition(runId: string, changeId: number): Promise<void>;
  initializeCheckpoints(runId: string): Promise<void>;
  checkpoint(runId: string, table: TableName): Promise<Checkpoint>;
  checkpoints(runId: string): Promise<Checkpoint[]>;
  completeTable(runId: string, table: TableName): Promise<void>;
  determineResumeStage(runId: string): Promise<ResumableStage>;
  setResumeStage(runId: string, stage: ResumableStage): Promise<void>;
  setPrimary(runId: string, database: PrimaryDatabase): Promise<void>;
}

export interface LabSettingsRepository {
  activeDatabase(): Promise<PrimaryDatabase>;
  writesFrozen(): Promise<boolean>;
  setWritesFrozen(frozen: boolean): Promise<void>;
  switchPrimary(database: PrimaryDatabase): Promise<void>;
  incrementPostgresOnlyWrites(): Promise<void>;
  postgresOnlyWrites(): Promise<number>;
  latestValidation(): Promise<ValidationResult | null>;
  saveValidation(result: ValidationResult): Promise<void>;
}

export interface EventRepository {
  append(category: string, level: string, message: string, details?: unknown): Promise<void>;
  recent(limit: number): Promise<readonly Record<string, unknown>[]>;
  after(id: number, limit: number): Promise<readonly Record<string, unknown>[]>;
}
