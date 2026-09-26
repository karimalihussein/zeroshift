package io.zeroshift.resilience;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * One resilience experiment: a hypothesis about the running system, the failure or load that tests
 * it, the mitigation, and checks that decide each claim from measured {@link Sample}s. Steps always
 * run in the same order: Hypothesis → Inject → Observe → Explain → Mitigate → Recover → Verify.
 */
public record Experiment(
    String id,
    String title,
    String summary,
    List<String> concepts,
    boolean needsChaos,
    LoadGenerator.Profile load,
    List<Step> steps) {

  public enum Phase {
    HYPOTHESIS,
    INJECT,
    OBSERVE,
    EXPLAIN,
    MITIGATE,
    RECOVER,
    VERIFY
  }

  /**
   * @param action what the step changes, returning a line saying what it did; null for none
   * @param settleSeconds how long to let the system run after the action before checking
   */
  public record Step(
      Phase phase,
      String title,
      String text,
      Action action,
      List<Check> checks,
      int settleSeconds) {}

  @FunctionalInterface
  public interface Action {
    String run() throws Exception;
  }

  /**
   * A claim decided from samples. Normally it is re-evaluated every second until it holds or {@code
   * withinSeconds} have passed since the step started. A {@code throughout} claim ("no dead
   * letters", "the API kept answering") is about the whole step instead: it is judged once, after
   * the step's other claims are decided and at least {@code withinSeconds} into the step, over
   * every sample of the step. The observation is shown either way.
   */
  public record Check(
      String claim, int withinSeconds, Function<Window, Observation> probe, boolean throughout) {
    public Check(String claim, int withinSeconds, Function<Window, Observation> probe) {
      this(claim, withinSeconds, probe, false);
    }
  }

  public record Observation(boolean holds, String observed) {
    static Observation of(boolean holds, String format, Object... values) {
      return new Observation(holds, String.format(java.util.Locale.ROOT, format, values));
    }

    static Observation unmeasured(String what) {
      return new Observation(false, what + " not measured yet");
    }
  }

  /** A measured quantity of one sample; null when that second did not measure it. */
  @FunctionalInterface
  public interface Metric extends Function<Sample, Double> {}

  /**
   * The samples a check reads: this step's so far, and every earlier step's by phase. The baseline
   * is the Hypothesis step's samples, taken before anything was broken.
   */
  public static final class Window {
    private final List<Sample> current;
    private final Map<Phase, List<Sample>> byPhase;

    public Window(List<Sample> current, Map<Phase, List<Sample>> byPhase) {
      this.current = current;
      this.byPhase = byPhase;
    }

    public int seconds() {
      return current.size();
    }

    public Double latest(Metric m) {
      for (int i = current.size() - 1; i >= 0; i--) {
        var v = m.apply(current.get(i));
        if (v != null) return v;
      }
      return null;
    }

    /** Mean over the last {@code n} samples of this step. */
    public Double recent(Metric m, int n) {
      return mean(current.subList(Math.max(0, current.size() - n), current.size()), m);
    }

    public Double max(Metric m) {
      return current.stream().map(m).filter(Objects::nonNull).max(Double::compare).orElse(null);
    }

    public Double sum(Metric m) {
      return current.stream()
          .map(m)
          .filter(Objects::nonNull)
          .mapToDouble(Double::doubleValue)
          .sum();
    }

    /** Sum over the last {@code n} samples of this step. */
    public double recentSum(Metric m, int n) {
      return current.subList(Math.max(0, current.size() - n), current.size()).stream()
          .map(m)
          .filter(Objects::nonNull)
          .mapToDouble(Double::doubleValue)
          .sum();
    }

    /** Value {@code secondsAgo} samples back in this step, or the first one. */
    public Double ago(Metric m, int secondsAgo) {
      if (current.isEmpty()) return null;
      return m.apply(current.get(Math.max(0, current.size() - 1 - secondsAgo)));
    }

    public Double baseline(Metric m) {
      return mean(byPhase.getOrDefault(Phase.HYPOTHESIS, List.of()), m);
    }

    /** The highest value seen during an earlier phase (or this one). */
    public Double peak(Metric m, Phase... phases) {
      var all = new ArrayList<Sample>();
      for (var p : phases) all.addAll(byPhase.getOrDefault(p, List.of()));
      return all.stream().map(m).filter(Objects::nonNull).max(Double::compare).orElse(null);
    }

    public Double sumOver(Metric m, Phase... phases) {
      var all = new ArrayList<Sample>();
      for (var p : phases) all.addAll(byPhase.getOrDefault(p, List.of()));
      return all.stream().map(m).filter(Objects::nonNull).mapToDouble(Double::doubleValue).sum();
    }

    private static Double mean(List<Sample> samples, Metric m) {
      var values = samples.stream().map(m).filter(Objects::nonNull).toList();
      return values.isEmpty()
          ? null
          : values.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
    }
  }
}
