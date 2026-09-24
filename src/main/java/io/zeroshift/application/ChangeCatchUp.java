package io.zeroshift.application;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import java.util.List;

public final class ChangeCatchUp {
  private final SourceDatabase source;
  private final int batchSize;

  public ChangeCatchUp(SourceDatabase source, int batchSize) {
    this.source = source;
    this.batchSize = batchSize;
  }

  public long drain(MigrationStore.Session session) {
    return drain(session, null);
  }

  /** Replay behind the snapshot frontier without advancing its original capture baseline. */
  public long drainCopied(MigrationStore.Session session) {
    return drain(session, session.state());
  }

  private long drain(MigrationStore.Session session, MigrationState frontier) {
    if (session.state().cdcPaused()) throw new InvalidAction("Resume CDC replay first");
    long applied = 0;
    try (var capture = source.capture(session.state().version())) {
      for (var table : Table.values()) {
        long after = 0;
        while (true) {
          var changes = capture.read(table, after, batchSize);
          if (changes.isEmpty()) break;
          after = changes.getLast().id();
          List<Change> safe =
              frontier == null
                  ? changes
                  : changes.stream().filter(c -> safeToReplay(frontier, table, c)).toList();
          applied += session.apply(table, safe);
        }
      }
      if (frontier == null || applied > 0)
        session.captured(frontier == null ? capture.version() : frontier.version(), applied);
    }
    return applied;
  }

  private boolean safeToReplay(MigrationState state, Table table, Change change) {
    long bound = table == Table.CUSTOMERS ? state.customerBound() : state.orderBound();
    long copiedThrough =
        table == Table.CUSTOMERS
            ? (state.table() == Table.CUSTOMERS ? state.lastId() : state.customerBound())
            : (state.table() == Table.ORDERS ? state.lastId() : 0);
    if (change.id() > copiedThrough && change.id() <= bound) return false;
    // New orders may reference an old customer whose snapshot page has not arrived yet.
    if (change.row() instanceof Row.Order order) {
      long customersThrough =
          state.table() == Table.CUSTOMERS ? state.lastId() : state.customerBound();
      return order.customerId() <= customersThrough || order.customerId() > state.customerBound();
    }
    return true;
  }

  public long pending(MigrationStore store) {
    var state = store.state();
    if (state.stage() == Stage.IDLE || state.stage() == Stage.COMPLETED) return 0;
    long pending = 0;
    try (var capture = source.capture(state.version())) {
      for (var table : Table.values()) {
        long after = 0;
        while (true) {
          var changes = capture.read(table, after, batchSize);
          if (changes.isEmpty()) break;
          for (var change : changes)
            if (store.appliedVersion(table, change.id()) < change.version()) pending++;
          after = changes.getLast().id();
        }
      }
    }
    return pending;
  }
}
