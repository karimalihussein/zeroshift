package io.zeroshift.domain;

import java.util.List;

public enum TrafficOperation {
  INSERT,
  UPDATE,
  DELETE,
  READ;

  /** A read-heavy application mix: 4 reads, 3 updates, 2 inserts, 1 delete per 10 steps. */
  private static final List<TrafficOperation> MIX =
      List.of(READ, INSERT, UPDATE, READ, UPDATE, INSERT, READ, DELETE, UPDATE, READ);

  public static TrafficOperation forStep(long step) {
    return MIX.get(Math.floorMod(step, MIX.size()));
  }

  public Table table() {
    return switch (this) {
      case INSERT, DELETE -> Table.CUSTOMERS;
      case READ, UPDATE -> Table.ORDERS;
    };
  }
}
