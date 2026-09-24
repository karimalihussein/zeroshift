package io.zeroshift.domain;

public enum Stage {
  IDLE,
  SNAPSHOT,
  CATCH_UP,
  PREPARE,
  READY,
  FREEZE,
  COMPLETE;

  public boolean canPause() {
    return this != IDLE && this != COMPLETE;
  }
}
