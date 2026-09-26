package io.zeroshift.racelab.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;

/** What a completed run established, all derived from its recorded events and final rows. */
public record RunResult(
    RunMetrics metrics,
    InvariantResult invariant,
    List<RequestResult> requests,
    Map<String, Object> initialState,
    Map<String, Object> finalState,
    List<Highlight> highlights,
    Explanation explanation) {

  /** One request's fate. Latency runs from its first BEGIN to its final commit or rollback. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record RequestResult(
      String lane,
      String requestId,
      String orderId,
      String customerId,
      String role,
      Outcome outcome,
      int attempts,
      long latencyMicros,
      long lockWaitMicros,
      long syncWaitMicros,
      String detail) {}

  public enum Outcome {
    /** The request's change committed (reserved, charged, deposited…). */
    SUCCEEDED,
    /** The request saw it could not proceed (no stock, already paid) and changed nothing. */
    REJECTED,
    /** PostgreSQL or the version check aborted it and no retry succeeded. */
    ABORTED,
    /** An unexpected error. */
    FAILED
  }

  /**
   * Measured over the run's requests. Lock waits come from PostgreSQL; sync waits are the lab's own
   * choreography (CONTROLLED interleaving) and are reported apart so they are never mistaken for
   * contention.
   */
  public record RunMetrics(
      int requests,
      int succeeded,
      int rejected,
      int aborted,
      int conflicts,
      int retries,
      int lockWaits,
      long lockWaitMicros,
      int deadlocks,
      int serializationFailures,
      int versionConflicts,
      long p50Micros,
      long p95Micros,
      long p99Micros,
      long maxMicros,
      long durationMicros,
      double throughputPerSecond,
      long syncWaitMicros) {}

  /** The experiment's rule, checked against the committed rows after every request finished. */
  public record InvariantResult(
      String statement, boolean holds, String expected, String actual, String detail) {}

  /**
   * A moment worth pointing at on the timeline: two requests reading the same value, a lock wait, a
   * rejected stale version… {@code seqs} are the events it is made of.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record Highlight(
      String kind,
      String tone,
      long atMicros,
      Long untilMicros,
      List<String> lanes,
      List<Integer> seqs,
      String title,
      String detail) {}

  /** "Why did this happen?", told from the captured events. */
  public record Explanation(
      String headline, List<Step> steps, String conclusion, String mechanism, Fix fix) {
    public record Step(int seq, String lane, String text, String tone) {}

    /** The configuration that fixes this run, and why; null when the run already holds. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Fix(Mode mode, Isolation isolation, String label, String why) {}
  }
}
