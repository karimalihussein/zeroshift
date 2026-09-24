package io.zeroshift.domain;

public enum TrafficOperation {
  INSERT,
  UPDATE,
  DELETE;

  public static TrafficOperation forStep(long step) {
    return switch (Math.floorMod(step, 3)) {
      case 0 -> INSERT;
      case 1 -> UPDATE;
      case 2 -> DELETE;
      default -> throw new IllegalStateException("Unreachable traffic operation");
    };
  }
}
