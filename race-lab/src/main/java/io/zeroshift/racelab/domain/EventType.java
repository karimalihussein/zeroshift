package io.zeroshift.racelab.domain;

/** Everything the engine records about a run. The UI draws these; it never parses log text. */
public enum EventType {
  EXPERIMENT_STARTED,
  /** A committed value of the shared data, read by the engine's own connection (database lane). */
  STATE_OBSERVED,
  TRANSACTION_STARTED,
  READ_PERFORMED,
  /** The application's check on what it read (stock &gt; 0, status = PAID…). */
  DECISION_MADE,
  /** The configured artificial delay between reading and writing (application work). */
  DELAY,
  /** The lab's choreography held this request at a named point (CONTROLLED interleaving only). */
  SYNC_POINT,
  LOCK_REQUESTED,
  LOCK_ACQUIRED,
  /** PostgreSQL reports the request's backend waiting on another's lock (pg_blocking_pids). */
  TRANSACTION_BLOCKED,
  TRANSACTION_UNBLOCKED,
  WRITE_PERFORMED,
  /** An optimistic write matched no row: the version it expected is no longer current. */
  VERSION_CONFLICT,
  /** SQLSTATE 40001: PostgreSQL aborted the transaction to keep the schedule serializable. */
  SERIALIZATION_FAILURE,
  /** SQLSTATE 40P01: PostgreSQL broke a wait-for cycle by aborting this transaction. */
  DEADLOCK_DETECTED,
  TRANSACTION_COMMITTED,
  TRANSACTION_ROLLED_BACK,
  LOCK_RELEASED,
  RETRY_SCHEDULED,
  /** A request's final answer: reserved, rejected, aborted… */
  REQUEST_COMPLETED,
  INVARIANT_CHECKED,
  EXPERIMENT_COMPLETED,
  EXPERIMENT_FAILED
}
