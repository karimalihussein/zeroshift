package io.zeroshift.application;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Replays post-cutover PostgreSQL changes into SQL Server. Each page is committed in SQL Server
 * first and acknowledged in PostgreSQL second, so a failure between the two replays the same net
 * images again: an upsert of a current row or a delete by key, both idempotent.
 *
 * <p>SQL Server enforces the orders → customers key immediately, so one pass applies customer
 * upserts, then order upserts and deletes, then customer deletes. The pass reads a single
 * PostgreSQL snapshot, so every order it sees has its customer in the same snapshot.
 */
public final class ReverseCatchUp {
  private final MigrationStore store;
  private final SourceDatabase source;
  private final SourceWriteback writeback;
  private final int batchSize;

  public ReverseCatchUp(
      MigrationStore store, SourceDatabase source, SourceWriteback writeback, int batchSize) {
    this.store = store;
    this.source = source;
    this.writeback = writeback;
    this.batchSize = batchSize;
  }

  /**
   * Keys SQL Server changed outside reverse sync since cutover. Checked for the whole table, so
   * rows PostgreSQL never touched cannot diverge silently.
   */
  public List<ReverseConflict> conflicts(long baselineVersion) {
    var conflicts = new ArrayList<ReverseConflict>();
    try (var capture = source.capture(baselineVersion)) {
      for (var table : Table.values())
        capture.outOfBand(table).forEach(id -> conflicts.add(new ReverseConflict(table, id)));
    }
    return conflicts;
  }

  /** Returns the conflicts that stopped the pass; empty when every captured change was applied. */
  public List<ReverseConflict> drain(MigrationStore.Session session) {
    long baseline = store.rollback().baselineVersion();
    long applied = 0;
    var customerDeletes = new ArrayList<Change>();
    try (var capture = store.reverseCapture()) {
      for (var table : List.of(Table.CUSTOMERS, Table.ORDERS)) {
        long after = 0;
        while (true) {
          var changes = capture.changes(table, after, batchSize);
          if (changes.isEmpty()) break;
          after = changes.getLast().id();
          var now = changes;
          if (table == Table.CUSTOMERS) {
            now = changes.stream().filter(c -> c.operation() != Change.Operation.DELETE).toList();
            changes.stream()
                .filter(c -> c.operation() == Change.Operation.DELETE)
                .forEach(customerDeletes::add);
          }
          var conflicts = apply(session, table, now, baseline);
          if (!conflicts.isEmpty()) return finish(session, applied, conflicts);
          applied += now.size();
        }
      }
      for (int from = 0; from < customerDeletes.size(); from += batchSize) {
        var page =
            customerDeletes.subList(from, Math.min(customerDeletes.size(), from + batchSize));
        var conflicts = apply(session, Table.CUSTOMERS, page, baseline);
        if (!conflicts.isEmpty()) return finish(session, applied, conflicts);
        applied += page.size();
      }
    }
    return finish(session, applied, List.of());
  }

  private List<ReverseConflict> apply(
      MigrationStore.Session session, Table table, List<Change> changes, long baseline) {
    if (changes.isEmpty()) return List.of();
    var conflicts = writeback.apply(table, changes, baseline);
    if (conflicts.isEmpty()) session.acknowledgeReverse(table, changes);
    return conflicts;
  }

  private List<ReverseConflict> finish(
      MigrationStore.Session session, long applied, List<ReverseConflict> conflicts) {
    session.reverseApplied(applied);
    return conflicts;
  }
}
