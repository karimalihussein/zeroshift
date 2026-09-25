package io.zeroshift.order.web;

import io.zeroshift.platform.Faults;
import org.springframework.stereotype.Component;

/**
 * The client-retry lab's fault: when armed, the answer to POST /orders is held back after the order
 * has committed, so a client with a short timeout gives up on a request that succeeded.
 */
@Component
public class SlowAnswers {
  /** Faults entry: how many ms to hold the answer. */
  public static final String FAULT = "slow-response";

  private final Faults faults;

  public SlowAnswers(Faults faults) {
    this.faults = faults;
  }

  void holdIfArmed() throws InterruptedException {
    var delay = faults.trigger(FAULT);
    if (delay.isPresent()) Thread.sleep(Long.parseLong(delay.get()));
  }
}
