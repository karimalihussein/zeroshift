package io.zeroshift.racelab.application;

import io.zeroshift.racelab.application.port.RunRepository;
import io.zeroshift.racelab.domain.RaceLabErrors;
import io.zeroshift.racelab.domain.Run;
import io.zeroshift.racelab.domain.RunConfig;
import io.zeroshift.racelab.domain.RunResult.InvariantResult;
import io.zeroshift.racelab.domain.RunResult.RunMetrics;
import java.util.ArrayList;
import java.util.List;

/**
 * Two runs side by side: the interleaving each one recorded, and what the difference in strategy
 * cost or bought, measured (correctness, successes, conflicts, retries, lock waits, latency).
 */
public class RunComparison {
  public record Side(
      long runId,
      String experiment,
      RunConfig config,
      Run.Status status,
      InvariantResult invariant,
      RunMetrics metrics,
      List<String> sequence) {}

  /** One measured difference. {@code better} says which side wins it, or null for a tie. */
  public record Difference(String metric, String unit, double left, double right, String better) {}

  public record Comparison(Side left, Side right, List<Difference> differences, String verdict) {}

  private static final int MAX_EVENTS = 5000;

  private final RunRepository runs;

  public RunComparison(RunRepository runs) {
    this.runs = runs;
  }

  public Comparison compare(long leftId, long rightId) {
    var left = side(leftId);
    var right = side(rightId);
    var diffs = new ArrayList<Difference>();
    if (left.metrics() != null && right.metrics() != null) {
      var l = left.metrics();
      var r = right.metrics();
      diffs.add(
          diff(
              "Invariant held",
              "",
              left.invariant().holds() ? 1 : 0,
              right.invariant().holds() ? 1 : 0,
              true));
      diffs.add(diff("Successful requests", "", l.succeeded(), r.succeeded(), null));
      diffs.add(diff("Rejected requests", "", l.rejected(), r.rejected(), null));
      diffs.add(diff("Conflicts", "", l.conflicts(), r.conflicts(), false));
      diffs.add(diff("Retries", "", l.retries(), r.retries(), false));
      diffs.add(diff("Lock waits", "", l.lockWaits(), r.lockWaits(), false));
      diffs.add(diff("Time waiting for locks", "ms", l.lockWaitMicros() / 1000.0, r.lockWaitMicros() / 1000.0, false));
      diffs.add(diff("Latency p50", "ms", l.p50Micros() / 1000.0, r.p50Micros() / 1000.0, false));
      diffs.add(diff("Latency p95", "ms", l.p95Micros() / 1000.0, r.p95Micros() / 1000.0, false));
      diffs.add(diff("Latency p99", "ms", l.p99Micros() / 1000.0, r.p99Micros() / 1000.0, false));
      diffs.add(diff("Throughput", "ok/s", l.throughputPerSecond(), r.throughputPerSecond(), true));
    }
    return new Comparison(left, right, diffs, verdict(left, right));
  }

  private Side side(long id) {
    var run = runs.find(id).orElseThrow(() -> new RaceLabErrors.RunNotFound(id));
    var result = run.result();
    return new Side(
        run.id(),
        run.experiment(),
        run.config(),
        run.status(),
        result == null ? null : result.invariant(),
        result == null ? null : result.metrics(),
        RunAnalysis.sequence(runs.events(id, 0, MAX_EVENTS)));
  }

  /** {@code higherIsBetter} null: neither direction is better (a count to read, not to win). */
  private static Difference diff(
      String metric, String unit, double left, double right, Boolean higherIsBetter) {
    String better = null;
    if (higherIsBetter != null && left != right)
      better = (left > right) == higherIsBetter ? "left" : "right";
    return new Difference(metric, unit, round(left), round(right), better);
  }

  private static double round(double v) {
    return Math.round(v * 10) / 10.0;
  }

  private static String verdict(Side left, Side right) {
    if (left.invariant() == null || right.invariant() == null)
      return "Both runs must complete before they can be compared.";
    boolean l = left.invariant().holds();
    boolean r = right.invariant().holds();
    if (l != r) {
      var safe = l ? left : right;
      var unsafe = l ? right : left;
      long extra = safe.metrics().p95Micros() - unsafe.metrics().p95Micros();
      return safe.config().mode().label()
          + " kept the invariant that "
          + unsafe.config().mode().label()
          + " broke"
          + (extra > 0
              ? ", at " + TransactionParticipant.ms(extra) + " more p95 latency"
              : "")
          + (safe.metrics().conflicts() > 0
              ? " and " + safe.metrics().conflicts() + " conflict(s) to absorb"
              : "")
          + (safe.metrics().lockWaits() > 0
              ? " and " + safe.metrics().lockWaits() + " lock wait(s)"
              : "")
          + ".";
    }
    return l
        ? "Both kept the invariant; compare what each paid for it: waits, conflicts, retries, latency."
        : "Both broke the invariant.";
  }
}
