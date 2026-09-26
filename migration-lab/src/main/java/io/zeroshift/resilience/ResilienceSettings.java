package io.zeroshift.resilience;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Where the resilience lab's chaos proxy lives. Blank when the Compose chaos overlay is not
 * running: the network experiments then say how to start it, and everything else (load,
 * backpressure, guards) still works against the direct connections.
 */
@ConfigurationProperties("resilience-lab")
public record ResilienceSettings(String toxiproxyUrl) {
  public boolean chaosConfigured() {
    return toxiproxyUrl != null && !toxiproxyUrl.isBlank();
  }
}
