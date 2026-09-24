package io.zeroshift.platform;

/** A handler's result: what it decided, and a human-readable reason for the control plane. */
public record Handled(Decision decision, String detail) {
  public static Handled processed(String detail) {
    return new Handled(Decision.PROCESSED, detail);
  }

  public static Handled ignored(String detail) {
    return new Handled(Decision.IGNORED, detail);
  }
}
