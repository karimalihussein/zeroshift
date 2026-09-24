export const TABLE_NAMES = ['customers', 'products', 'orders', 'order_items', 'payments'] as const;

export type TableName = (typeof TABLE_NAMES)[number];
export type ChangeOperation = 'I' | 'U' | 'D';
export type MigrationSpeed = 'real' | 'normal' | 'slow';
export type PrimaryDatabase = 'sqlserver' | 'postgres';

export const MIGRATION_STATES = [
  'IDLE',
  'PREPARING',
  'CAPTURING_CHANGES',
  'INITIAL_COPY',
  'CATCHING_UP',
  'BUILDING_INDEXES',
  'SYNCING_SEQUENCES',
  'ANALYZING',
  'VALIDATING',
  'READY_FOR_CUTOVER',
  'FREEZING_WRITES',
  'FINAL_SYNC',
  'CUTTING_OVER',
  'MONITORING',
  'COMPLETED',
  'FAILED',
  'PAUSED',
] as const;

export type MigrationState = (typeof MIGRATION_STATES)[number];
export type ResumableStage = 'INITIAL_COPY' | 'CATCHING_UP';

export interface MigrationConfig {
  batchSize: number;
  cdcBatchSize: number;
  speed: MigrationSpeed;
}

export interface MigrationRun {
  id: string;
  state: MigrationState;
  snapshotBoundary: number;
  lastAppliedChange: number;
  activeDatabase: PrimaryDatabase;
  writesFrozen: boolean;
  config: MigrationConfig;
  resumeStage: ResumableStage | null;
}

export interface Checkpoint {
  runId: string;
  tableName: TableName;
  lastProcessedId: number;
  rowsProcessed: number;
  batchNumber: number;
  status: 'pending' | 'running' | 'complete';
  updatedAt: Date;
}

export type DatabaseRow = Record<string, unknown> & { id: number };

export interface ChangeEvent {
  changeId: number;
  tableName: TableName;
  operation: ChangeOperation;
  recordId: number;
  rowData: DatabaseRow | null;
  changedAt: Date;
}

export interface TrafficStatistics {
  insert: number;
  update: number;
  delete: number;
  errors: number;
}

export interface RuntimeSnapshot {
  trafficRunning: boolean;
  trafficRate: number;
  trafficStats: TrafficStatistics;
  failNextBatch: boolean;
  networkDelay: number;
  cdcPaused: boolean;
  workerAbort: boolean;
  rowsPerSecond: number;
  lastBatchMs: number;
}

export interface TableValidationResult {
  table: TableName;
  sourceCount: number;
  targetCount: number;
  countMatch: boolean;
  aggregatesMatch: boolean;
  checksumMatch: boolean;
  passed: boolean;
}

export interface ValidationResult {
  passed: boolean;
  tables: TableValidationResult[];
  relationships: boolean;
  checkedAt: string;
  levels: readonly string[];
}
