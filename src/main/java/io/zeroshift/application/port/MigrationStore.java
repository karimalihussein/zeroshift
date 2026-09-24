package io.zeroshift.application.port;

import io.zeroshift.domain.*;
import java.util.List;
import java.util.function.Function;

public interface MigrationStore {
  MigrationState state();

  <T> T transaction(Function<Session, T> work);

  List<String> logs();

  long count(Table table);

  long appliedVersion(Table table, long id);

  List<Row> read(Table table, long afterId, int limit);

  TrafficMetrics trafficMetrics();

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

    void complete();

    void traffic(boolean enabled);

    void writeTraffic(TrafficOperation operation);

    /** Counts one committed operation against the database it was routed to. */
    void recordTraffic(TrafficOperation operation, Primary target);

    /** Counts one failed operation and returns the current run of consecutive failures. */
    int trafficError();

    void reset();

    void log(String message);
  }
}
