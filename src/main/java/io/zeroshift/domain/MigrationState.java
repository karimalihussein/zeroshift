package io.zeroshift.domain;

import java.time.Instant;

/** Immutable durable state. Stage survives pause, failure and process death. */
public record MigrationState(
    Stage stage,
    RunStatus status,
    Primary primary,
    Table table,
    long lastId,
    long customerBound,
    long orderBound,
    long version,
    long copied,
    long expected,
    long batches,
    long applied,
    boolean traffic,
    String validation,
    String error,
    Instant checkpoint,
    double rowsPerSecond,
    boolean cdcPaused) {
  public double progress() {
    return switch (stage) {
      case IDLE -> 0;
      case SNAPSHOT -> Math.min(80, expected == 0 ? 80 : copied * 80.0 / expected);
      case CATCH_UP -> 85;
      case PREPARE -> 90;
      case READY -> 95;
      case FREEZE -> 98;
      case COMPLETE -> 100;
    };
  }

  public long bound() {
    return table == Table.CUSTOMERS ? customerBound : orderBound;
  }

  public boolean active() {
    return status == RunStatus.RUNNING;
  }

  public void require(boolean condition, String message) {
    if (!condition) throw new InvalidAction(message);
  }
}
