package io.zeroshift.application;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import java.util.ArrayList;
import java.util.List;

public final class LiveChangesService {
  public record CaptureInfo(
      Table table, long recordId, long version, String operation, boolean pending) {}

  public record Inspection(
      long orderId,
      OrderRecord source,
      OrderRecord target,
      LiveExperiment experiment,
      List<CaptureInfo> changes,
      boolean inSync,
      boolean cdcPaused,
      long ordersCopiedThrough,
      String message) {}

  private final MigrationStore store;
  private final SourceDatabase captureSource;
  private final LiveSource source;
  private final LiveTarget target;

  public LiveChangesService(
      MigrationStore store, SourceDatabase captureSource, LiveSource source, LiveTarget target) {
    this.store = store;
    this.captureSource = captureSource;
    this.source = source;
    this.target = target;
  }

  private void writable(MigrationState state) {
    state.require(
        state.active() && state.primary() == Primary.SQL_SERVER && state.stage() != Stage.FREEZE,
        "Live changes require an active migration with SQL Server as primary");
  }

  public void pauseReplay(boolean paused) {
    store.transaction(
        s -> {
          writable(s.state());
          s.cdcPaused(paused);
          return null;
        });
  }

  public LiveExperiment insert(OrderEdit edit) {
    return store.transaction(
        s -> {
          writable(s.state());
          var result = source.insert(edit);
          s.log("Manual INSERT Order #" + result.orderId() + " in SQL Server");
          return result;
        });
  }

  public LiveExperiment update(long id, OrderEdit edit) {
    return store.transaction(
        s -> {
          requireCopied(s.state(), id);
          var result = source.update(id, edit);
          s.log("Manual UPDATE Order #" + id + " in SQL Server");
          return result;
        });
  }

  public LiveExperiment delete(long id) {
    return store.transaction(
        s -> {
          requireCopied(s.state(), id);
          var result = source.delete(id);
          s.log("Manual DELETE Order #" + id + " in SQL Server");
          return result;
        });
  }

  private void requireCopied(MigrationState state, long id) {
    writable(state);
    state.require(
        id > 0 && target.order(id).isPresent(),
        "Choose an order that already exists in PostgreSQL");
  }

  public Inspection selectCopied() {
    return store.transaction(
        s -> {
          writable(s.state());
          var candidates = target.candidates();
          // Prefer an interior key, away from the simulator's MIN/MAX targets.
          for (int i = candidates.size() - 1; i >= 0; i--)
            if (source.order(candidates.get(i)).isPresent())
              return inspectLocked(candidates.get(i), s.state());
          throw new InvalidAction(
              "No migrated order is available yet. Wait for the orders snapshot to begin.");
        });
  }

  public Inspection inspect(long id) {
    if (id < 1) throw new InvalidAction("Order ID must be positive");
    return store.transaction(s -> inspectLocked(id, s.state()));
  }

  private Inspection inspectLocked(long id, MigrationState state) {
    var sourceRow = source.order(id).orElse(null);
    var targetRow = target.order(id).orElse(null);
    var experiment = source.latest(id).orElse(null);
    var changes = new ArrayList<CaptureInfo>();
    if (state.stage() != Stage.IDLE) {
      // Query from the baseline for pending keys; receipts retain evidence after it advances.
      try (var capture = captureSource.capture(state.version())) {
        capture(changes, capture, Table.ORDERS, id);
        long customerId =
            sourceRow != null
                ? sourceRow.customerId()
                : targetRow != null
                    ? targetRow.customerId()
                    : experiment != null
                        ? (experiment.before() != null ? experiment.before() : experiment.after())
                            .customerId()
                        : 0;
        if (customerId > 0) capture(changes, capture, Table.CUSTOMERS, customerId);
      }
    }
    boolean equal =
        sourceRow == null
            ? targetRow == null && experiment != null
            : sourceRow.sameValues(targetRow);
    boolean pending = changes.stream().anyMatch(CaptureInfo::pending);
    long frontier =
        state.stage() == Stage.IDLE
                || (state.stage() == Stage.SNAPSHOT && state.table() == Table.CUSTOMERS)
            ? 0
            : state.stage() == Stage.SNAPSHOT ? state.lastId() : state.orderBound();
    String message =
        sourceRow == null && targetRow == null && experiment == null
            ? "Order not found in either database"
            : pending
                ? "CDC pending"
                : equal
                    ? (sourceRow == null ? "IN SYNC — absent in both databases" : "IN SYNC")
                    : (state.stage() == Stage.COMPLETE
                        ? "Databases differ after cutover"
                        : "Waiting for snapshot or CDC replay");
    return new Inspection(
        id,
        sourceRow,
        targetRow,
        experiment,
        List.copyOf(changes),
        equal && !pending,
        state.cdcPaused(),
        frontier,
        message);
  }

  private void capture(
      List<CaptureInfo> output, SourceDatabase.Capture capture, Table table, long id) {
    var next = capture.read(table, id - 1, 1);
    long receipt = store.appliedVersion(table, id);
    if (!next.isEmpty() && next.getFirst().id() == id) {
      var change = next.getFirst();
      output.add(
          new CaptureInfo(
              table, id, change.version(), change.operation().name(), receipt < change.version()));
    } else if (receipt > 0) output.add(new CaptureInfo(table, id, receipt, "REPLAYED", false));
  }
}
