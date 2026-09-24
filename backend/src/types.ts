export const TABLES = ['customers', 'products', 'orders', 'order_items', 'payments'] as const;
export type TableName = (typeof TABLES)[number];
export const STATES = [
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
export type MigrationState = (typeof STATES)[number];
export interface MigrationConfig {
  batchSize: number;
  cdcBatchSize: number;
  speed: 'real' | 'normal' | 'slow';
}
export interface Runtime {
  trafficRunning: boolean;
  trafficRate: number;
  trafficStats: { insert: number; update: number; delete: number; errors: number };
  failNextBatch: boolean;
  networkDelay: number;
  cdcPaused: boolean;
  workerAbort: boolean;
  previousState?: MigrationState;
  rowsPerSecond: number;
  lastBatchMs: number;
}
export interface EventRow {
  id: number;
  category: string;
  level: string;
  message: string;
  details: unknown;
  created_at: string;
}
