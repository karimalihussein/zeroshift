import { InvalidTransitionError } from './errors.js';
import type { MigrationState } from './models.js';

const transitions: Record<MigrationState, readonly MigrationState[]> = {
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
  FAILED: ['PREPARING', 'INITIAL_COPY', 'CATCHING_UP'],
  PAUSED: ['INITIAL_COPY', 'CATCHING_UP', 'FAILED'],
};

export class MigrationStateMachine {
  canTransition(from: MigrationState, to: MigrationState): boolean {
    return transitions[from].includes(to);
  }

  assertTransition(from: MigrationState, to: MigrationState): void {
    if (!this.canTransition(from, to)) throw new InvalidTransitionError(from, to);
  }
}
