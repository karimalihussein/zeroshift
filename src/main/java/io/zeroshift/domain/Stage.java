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
  COMPLETED;

  public boolean canPause() {
    return this == SNAPSHOT || this == CATCH_UP || this == PREPARE || this == READY;
  }

  public boolean canResume() {
    return this != IDLE && this != COMPLETED;
  }

  public boolean sourceMustRemainFrozen() {
    return this == FREEZE || this == VALIDATION || this == CUTOVER;
  }
}
