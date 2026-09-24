import type { RuntimeSnapshot, TrafficStatistics } from '../domain/models.js';

const emptyStatistics = (): TrafficStatistics => ({ insert: 0, update: 0, delete: 0, errors: 0 });

export class RuntimeState {
  private state: RuntimeSnapshot = {
    trafficRunning: false,
    trafficRate: 5,
    trafficStats: emptyStatistics(),
    failNextBatch: false,
    networkDelay: 0,
    cdcPaused: false,
    workerAbort: false,
    rowsPerSecond: 0,
    lastBatchMs: 0,
  };

  snapshot(): Readonly<RuntimeSnapshot> {
    return structuredClone(this.state);
  }

  beginWorker(): void {
    this.state.workerAbort = false;
  }

  abortWorker(): void {
    this.state.workerAbort = true;
  }

  shouldAbortWorker(): boolean {
    return this.state.workerAbort;
  }

  failNextBatch(): void {
    this.state.failNextBatch = true;
  }

  consumeBatchFailure(): boolean {
    if (!this.state.failNextBatch) return false;
    this.state.failNextBatch = false;
    return true;
  }

  setNetworkDelay(milliseconds: number): void {
    this.state.networkDelay = Math.max(0, milliseconds);
  }

  networkDelay(): number {
    return this.state.networkDelay;
  }

  setCdcPaused(paused: boolean): void {
    this.state.cdcPaused = paused;
  }

  isCdcPaused(): boolean {
    return this.state.cdcPaused;
  }

  setTraffic(running: boolean, rate?: number): void {
    this.state.trafficRunning = running;
    if (rate !== undefined) this.state.trafficRate = Math.max(1, Math.min(50, rate));
  }

  isTrafficRunning(): boolean {
    return this.state.trafficRunning;
  }

  trafficRate(): number {
    return this.state.trafficRate;
  }

  recordTraffic(operation: keyof Omit<TrafficStatistics, 'errors'>): void {
    this.state.trafficStats[operation] += 1;
  }

  recordTrafficError(): void {
    this.state.trafficStats.errors += 1;
  }

  recordBatch(rows: number, durationMs: number): void {
    this.state.lastBatchMs = durationMs;
    this.state.rowsPerSecond = Math.round(rows / Math.max(durationMs / 1000, 0.001));
  }

  reset(): void {
    this.state = {
      ...this.state,
      trafficRunning: false,
      trafficStats: emptyStatistics(),
      failNextBatch: false,
      networkDelay: 0,
      cdcPaused: false,
      workerAbort: true,
      rowsPerSecond: 0,
      lastBatchMs: 0,
    };
  }
}
