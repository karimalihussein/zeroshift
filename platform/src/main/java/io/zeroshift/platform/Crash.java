package io.zeroshift.platform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A real process death: no shutdown hooks, no graceful consumer close, no offset commit. */
public final class Crash {
  private static final Logger log = LoggerFactory.getLogger(Crash.class);

  public static void now(String reason) {
    log.error("Simulated crash: {}", reason);
    Runtime.getRuntime().halt(137);
  }

  private Crash() {}
}
