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
            if (!state.traffic() || state.stage() == Stage.FREEZE) return null;
            var operation = TrafficOperation.forStep(s.trafficStep());
            if (state.primary() == Primary.SQL_SERVER) source.writeTraffic(operation);
            else s.writeTraffic(operation);
            s.recordTraffic(operation, state.primary());
            return null;
          });
    } catch (RuntimeException e) {
      store.transaction(
          s -> {
            int failures = s.trafficError();
            if (failures == 1)
              s.log("Traffic operation failed; retrying: " + MigrationCoordinator.safeMessage(e));
            if (failures < MAX_CONSECUTIVE_ERRORS) return null;
            s.traffic(false);
            s.log(
                "Traffic stopped after "
                    + MAX_CONSECUTIVE_ERRORS
                    + " consecutive database errors: "
                    + MigrationCoordinator.safeMessage(e));
            return null;
          });
    }
  }
}
