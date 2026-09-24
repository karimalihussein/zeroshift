package io.zeroshift.domain;

public enum Stage {
  IDLE,
  SNAPSHOT,
  CATCH_UP,
  PREPARE,
  READY,
  FREEZE,
  VALIDATION,
  CUTOVER,
  COMPLETED,
  // Rollback after a successful cutover: PostgreSQL → SQL Server.
  ROLLBACK_PREPARE,
  REVERSE_CATCH_UP,
  ROLLBACK_VALIDATION,
  ROLLBACK_FREEZE,
  FINAL_SYNC,
  SWITCH_PRIMARY,
  ROLLED_BACK;

  public boolean canPause() {
    return this == SNAPSHOT
        || this == CATCH_UP
        || this == PREPARE
        || this == READY
        || this == ROLLBACK_PREPARE
        || this == REVERSE_CATCH_UP
        || this == ROLLBACK_VALIDATION;
  }

  public boolean canResume() {
    return this != IDLE && this != COMPLETED && this != ROLLED_BACK;
  }

  public boolean sourceMustRemainFrozen() {
    return this == FREEZE || this == VALIDATION || this == CUTOVER;
  }

  public boolean rollback() {
    return compareTo(ROLLBACK_PREPARE) >= 0;
  }

  /** Rollback stages that may still be abandoned: SQL Server has not been reopened. */
  public boolean rollbackAbortable() {
    return rollback() && compareTo(SWITCH_PRIMARY) < 0;
  }

  /** Application writes wait at the routing lock while either write fence is being set or held. */
  public boolean writesHeld() {
    return sourceMustRemainFrozen()
        || this == ROLLBACK_FREEZE
        || this == FINAL_SYNC
        || this == SWITCH_PRIMARY;
  }
}
