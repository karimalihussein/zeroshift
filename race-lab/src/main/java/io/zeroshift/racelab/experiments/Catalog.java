package io.zeroshift.racelab.experiments;

import io.zeroshift.racelab.application.Experiment;
import java.util.List;

/** Every experiment of the lab. */
public final class Catalog {
  private Catalog() {}

  public static List<Experiment> all() {
    return List.of(
        new Oversell(),
        new LostUpdate(),
        new DoublePayment(),
        new StateTransition(),
        new StaleEdit(),
        new LockQueue(),
        new Deadlock(),
        new NonRepeatableRead(),
        new PhantomRead(),
        new WriteSkew());
  }
}
