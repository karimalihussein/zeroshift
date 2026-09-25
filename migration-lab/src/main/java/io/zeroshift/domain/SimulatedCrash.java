package io.zeroshift.domain;

public final class SimulatedCrash extends MigrationException {
  public SimulatedCrash() {
    super(
        "Simulated worker crash: target transaction rolled back before checkpoint. Resume to replay.");
  }
}
