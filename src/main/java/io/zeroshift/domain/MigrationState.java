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
    boolean cdcPaused,
    boolean validationPassed,
    Instant startedAt,
    Instant cutoverStartedAt,
    Instant completedAt,
    long completedRows) {
  public double progress() {
    return switch (stage) {
      case IDLE -> 0;
      case SNAPSHOT -> expected <= 0 ? 0 : Math.min(80, copied * 80.0 / expected);
      case CATCH_UP -> 85;
      case PREPARE -> 90;
      case READY -> 95;
      case FREEZE -> 96;
      case VALIDATION -> 98;
      case CUTOVER -> 99;
      case COMPLETED -> successful() ? 100 : 99;
    };
  }

  public boolean successful() {
    return stage == Stage.COMPLETED
        && status == RunStatus.SUCCESS
        && primary == Primary.POSTGRESQL
        && validationPassed;
  }

  public Long durationMillis() {
    return startedAt == null || completedAt == null
        ? null
        : Math.max(0, java.time.Duration.between(startedAt, completedAt).toMillis());
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
