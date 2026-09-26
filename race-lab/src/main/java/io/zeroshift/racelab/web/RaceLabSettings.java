package io.zeroshift.racelab.web;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Where the page links a run's trace to: Grafana, whose Tempo datasource holds the spans. */
@ConfigurationProperties("race-lab")
public record RaceLabSettings(String grafanaUrl) {
  public RaceLabSettings {
    if (grafanaUrl == null || grafanaUrl.isBlank()) grafanaUrl = "http://localhost:3000";
  }
}
