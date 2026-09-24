package io.zeroshift.application;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;

public final class TrafficSimulator {
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

  public void tick() {
    try {
      store.transaction(
          s -> {
            var state = s.state();
            if (!state.traffic() || state.stage() == Stage.FREEZE) return null;
            var operation = TrafficOperation.forStep(s.trafficStep());
            if (state.primary() == Primary.SQL_SERVER) source.writeTraffic(operation);
            else s.writeTraffic(operation);
            return null;
          });
    } catch (RuntimeException e) {
      store.transaction(
          s -> {
            s.traffic(false);
            s.log("Traffic stopped after database error: " + MigrationCoordinator.safeMessage(e));
            return null;
          });
    }
  }
}
