package io.zeroshift.racelab.domain;

import java.util.List;

/**
 * An experiment as the lab presents it: what it teaches, the invariant it checks, and which
 * strategies and limits apply.
 *
 * @param invariant the rule every correct execution keeps, in one line
 * @param valueLabel what {@link RunConfig#initialValue()} means here ("Initial stock")
 * @param roles what each request does when they differ (reader/writer, ship/cancel); empty if all
 *     requests do the same thing
 */
public record ExperimentInfo(
    String id,
    int number,
    String title,
    String category,
    String question,
    String learn,
    String invariant,
    String valueLabel,
    List<ModeInfo> modes,
    Mode defaultMode,
    Isolation defaultIsolation,
    Limits limits,
    List<String> roles,
    String unsafeStory) {

  /** How one strategy works in this experiment, and the isolation it runs under by default. */
  public record ModeInfo(
      Mode mode, String label, String how, String sql, Isolation isolation, boolean fixes) {}

  public record Limits(
      int minRequests,
      int maxRequests,
      int defaultRequests,
      int minValue,
      int maxValue,
      int defaultValue,
      int defaultDelayMs,
      int defaultRetries) {}

  public ModeInfo mode(Mode mode) {
    return modes.stream().filter(m -> m.mode() == mode).findFirst().orElse(null);
  }
}
