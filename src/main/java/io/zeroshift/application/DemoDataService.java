package io.zeroshift.application;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;

public final class DemoDataService {
  public static final int MAX_ROWS = 10_000_000;
  private final SourceDatabase source;
  private final MigrationStore store;
  private final int seedRows;

  public DemoDataService(SourceDatabase source, MigrationStore store, int seedRows) {
    this.source = source;
    this.store = store;
    this.seedRows = seedRows;
  }

  public int defaultRows() {
    return seedRows;
  }

  /** Generates {@code rows} customers and orders, or the configured default when null. */
  public void seed(Integer rows) {
    int count = rows == null ? seedRows : rows;
    if (count < 1 || count > MAX_ROWS)
      throw new InvalidAction("Rows must be between 1 and " + String.format("%,d", MAX_ROWS));
    store.transaction(
        s -> {
          var state = s.state();
          state.require(
              state.stage() == Stage.IDLE && !state.traffic(),
              "Stop traffic and Reset before generating demo data");
          state.require(
              source.count(Table.CUSTOMERS) == 0 && source.count(Table.ORDERS) == 0,
              "Demo data already exists. Reset first.");
          source.seed(count);
          s.log("Generated " + count + " customers and " + count + " orders");
          return null;
        });
  }

  public void reset() {
    store.transaction(
        s -> {
          source.reset();
          s.reset();
          return null;
        });
  }
}
