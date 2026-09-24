package io.zeroshift.application;

import io.zeroshift.application.port.*;

public final class SnapshotBatch {
  private final SourceDatabase source;
  private final int batchSize;

  public SnapshotBatch(SourceDatabase source, int batchSize) {
    this.source = source;
    this.batchSize = batchSize;
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
  }
}
