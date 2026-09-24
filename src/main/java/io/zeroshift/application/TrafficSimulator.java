package io.zeroshift.application;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;

public final class TrafficSimulator {
  /** One failure is counted and retried; a run of them means the database is down. */
  static final int MAX_CONSECUTIVE_ERRORS = 5;

  private final MigrationStore store;
  private final SourceDatabase source;

  public TrafficSimulator(MigrationStore store, SourceDatabase source) {
    this.store = store;
    this.source = source;
  }

  public void toggle(boolean enabled) {
    store.transaction(
        s -> {
          if (enabled)
            s.state()
                .require(
                    !s.state().stage().sourceMustRemainFrozen(),
                    "Traffic cannot start while cutover has fenced SQL Server writes");
          s.traffic(enabled);
          return null;
        });
  }

  public TrafficMetrics metrics() {
    return store.trafficMetrics();
  }

  public void tick() {
    try {
      store.transaction(
          s -> {
            // Routing is read under the migration row lock, so cutover cannot interleave.
            var state = s.state();
            if (!state.traffic() || state.stage().sourceMustRemainFrozen()) return null;
            var operation = TrafficOperation.forStep(s.trafficStep());
            var target = state.primary();
            if (target == Primary.SQL_SERVER) {
              TrafficOperationOutcome outcome;
              try {
                outcome = source.writeTraffic(operation);
              } catch (RuntimeException e) {
                throw new TrafficFailure(operation, target, e);
              }
              // The source committed independently. Do not misreport a later log-store failure as
              // an operation failure.
              s.recordTraffic(TrafficOperationResult.success(operation, target, outcome));
            } else {
              try {
                var outcome = s.writeTraffic(operation);
                s.recordTraffic(TrafficOperationResult.success(operation, target, outcome));
              } catch (RuntimeException e) {
                // Both the PostgreSQL operation and its result are in this transaction and roll
                // back together, so recording a failed operation is accurate.
                throw new TrafficFailure(operation, target, e);
              }
            }
            return null;
          });
    } catch (TrafficFailure failure) {
      store.transaction(
          s -> {
            var result =
                TrafficOperationResult.failure(
                    failure.operation,
                    failure.target,
                    MigrationCoordinator.safeMessage(failure.cause()));
            int failures = s.trafficError(result);
            if (failures < MAX_CONSECUTIVE_ERRORS) return null;
            s.traffic(false);
            s.log(
                "Traffic stopped after "
                    + MAX_CONSECUTIVE_ERRORS
                    + " consecutive database errors: "
                    + result.details());
            return null;
          });
    }
  }

  private static final class TrafficFailure extends RuntimeException {
    private final TrafficOperation operation;
    private final Primary target;

    private TrafficFailure(TrafficOperation operation, Primary target, RuntimeException cause) {
      super(cause);
      this.operation = operation;
      this.target = target;
    }

    private RuntimeException cause() {
      return (RuntimeException) getCause();
    }
  }
}
