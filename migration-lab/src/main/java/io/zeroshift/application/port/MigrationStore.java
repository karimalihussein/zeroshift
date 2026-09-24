package io.zeroshift.application.port;

import io.zeroshift.domain.*;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public interface MigrationStore {
  MigrationState state();

  <T> T transaction(Function<Session, T> work);

  List<String> logs();

  /** The same newest-first lines as {@link #logs()}, with their id and exact time. */
  List<LogEvent> logEvents();

  long count(Table table);

  long appliedVersion(Table table, long id);

  List<Row> read(Table table, long afterId, int limit);

  TrafficMetrics trafficMetrics();

  RollbackState rollback();

  /** The highest key PostgreSQL's identity sequence has handed out, 0 if none. */
  long issuedKey(Table table);

  /** Distinct keys captured on PostgreSQL since cutover and not yet acknowledged by SQL Server. */
  long reversePending();

  /** Opens one read-only PostgreSQL snapshot over captured changes and current rows. */
  ReverseCapture reverseCapture();

  /**
   * Post-cutover changes as net images: each captured key maps to its current row, or to a delete
   * when the row is gone. {@link Change#version()} is the highest captured sequence for that key.
   */
  interface ReverseCapture extends AutoCloseable {
    List<Change> changes(Table table, long afterId, int limit);

    List<Row> rows(Table table, long afterId, int limit);

    Set<Long> pendingKeys(Table table);

    long pending();

    @Override
    void close();
  }

  interface Session {
    MigrationState state();

    void start(SourceDatabase.Boundary boundary);

    void requestCrash();

    void checkCrash();

    long trafficStep();

    void stage(Stage stage);

    void status(RunStatus status, String error);

    void snapshot(List<Row> rows, long lastId, long startedNanos);

    void nextTable();

    long apply(Table table, List<Change> changes);

    void cdcPaused(boolean paused);

    void captured(long version, long applied);

    void prepare();

    void synchronizeSequences();

    void validation(ValidationResult result);

    void beginCutover();

    /** Makes PostgreSQL primary, recording the CDC backlog measured behind the source fence. */
    void complete(long cdcPending);

    void traffic(boolean enabled);

    TrafficOperationOutcome writeTraffic(TrafficOperation operation);

    /** Counts and logs one committed operation against the database it was routed to. */
    void recordTraffic(TrafficOperationResult result);

    /** Counts and logs one failed operation and returns the current consecutive failure run. */
    int trafficError(TrafficOperationResult result);

    void beginRollback();

    /**
     * Deletes captured entries up to each change's version. Call only after SQL Server committed
     * those images; a crash before this commit replays them, which is idempotent.
     */
    void acknowledgeReverse(Table table, List<Change> changes);

    void reverseApplied(long applied);

    void conflicts(List<ReverseConflict> conflicts);

    void rollbackValidation(ValidationResult result, String scope);

    /** Sets PostgreSQL's write fence. Setting it waits for in-flight guarded writes to commit. */
    void freezeWrites(boolean frozen);

    void completeRollback();

    void abortRollback();

    void reset();

    void log(String message);
  }
}
