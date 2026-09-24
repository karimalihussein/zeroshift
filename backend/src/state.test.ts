import { describe, expect, it } from 'vitest';
import { isValidTransition } from './state.js';
describe('migration state machine', () => {
  it('allows the safe lifecycle', () => {
    expect(isValidTransition('IDLE', 'PREPARING')).toBe(true);
    expect(isValidTransition('INITIAL_COPY', 'CATCHING_UP')).toBe(true);
    expect(isValidTransition('READY_FOR_CUTOVER', 'FREEZING_WRITES')).toBe(true);
  });
  it('rejects early cutover', () => {
    expect(isValidTransition('INITIAL_COPY', 'CUTTING_OVER')).toBe(false);
    expect(isValidTransition('CATCHING_UP', 'CUTTING_OVER')).toBe(false);
  });
  it('allows pause and recovery only in worker stages', () => {
    expect(isValidTransition('INITIAL_COPY', 'PAUSED')).toBe(true);
    expect(isValidTransition('PAUSED', 'INITIAL_COPY')).toBe(true);
    expect(isValidTransition('ANALYZING', 'PAUSED')).toBe(false);
  });
});
