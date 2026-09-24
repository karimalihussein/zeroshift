package io.zeroshift.application;

import io.zeroshift.application.port.*;
import java.time.Duration;

public final class SnapshotBatch {
  private final SourceDatabase source;
  private final int batchSize;
  private final MigrationMetrics metrics;

  public SnapshotBatch(SourceDatabase source, int batchSize, MigrationMetrics metrics) {
    this.source = source;
    this.batchSize = batchSize;
    this.metrics = metrics;
  }

  public void copy(MigrationStore.Session session) {
    var state = session.state();
    long started = System.nanoTime();
    var rows = source.read(state.table(), state.lastId(), state.bound(), batchSize);
    if (rows.isEmpty()) {
      session.checkCrash();
      session.nextTable();
      return;
    }
    session.snapshot(rows, rows.getLast().id(), started);
    metrics.batchCopied(state.table(), Duration.ofNanos(System.nanoTime() - started));
  }
}
