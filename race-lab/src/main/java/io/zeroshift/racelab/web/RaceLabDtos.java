package io.zeroshift.racelab.web;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.zeroshift.racelab.domain.Interleaving;
import io.zeroshift.racelab.domain.Isolation;
import io.zeroshift.racelab.domain.Mode;
import io.zeroshift.racelab.domain.Run;
import io.zeroshift.racelab.domain.RunConfig;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;
import java.util.Map;

/** The race lab's request and response records. */
public final class RaceLabDtos {
  private RaceLabDtos() {}

  /**
   * A run to create. Everything but the experiment is optional: an omitted value takes the
   * experiment's default (its default mode, the mode's isolation level, its request count…).
   */
  public record CreateRun(
      @NotBlank @Pattern(regexp = "[a-z-]{1,40}") String experiment,
      Mode mode,
      Isolation isolation,
      @Min(1) @Max(20) Integer requests,
      @Min(0) @Max(100_000) Integer initialValue,
      @Min(0) @Max(5000) Integer delayMs,
      Interleaving interleaving,
      @Min(0) @Max(10) Integer maxRetries) {
    RunConfig toConfig() {
      return new RunConfig(
          experiment,
          mode,
          isolation,
          requests == null ? -1 : requests,
          initialValue == null ? Integer.MIN_VALUE : initialValue,
          delayMs == null ? -1 : delayMs,
          interleaving,
          maxRetries == null ? -1 : maxRetries);
    }
  }

  /** A run in a list: enough to recognise it and pick it for a comparison. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record RunSummary(
      long id,
      String experiment,
      Mode mode,
      Isolation isolation,
      int requests,
      int initialValue,
      int delayMs,
      Interleaving interleaving,
      Run.Status status,
      Instant createdAt,
      Instant completedAt,
      Boolean invariantHolds,
      Integer succeeded,
      Integer rejected,
      Integer aborted,
      Integer conflicts,
      Integer lockWaits,
      Long p95Micros,
      String traceId) {
    static RunSummary of(Run run) {
      var c = run.config();
      var r = run.result();
      var m = r == null ? null : r.metrics();
      return new RunSummary(
          run.id(),
          run.experiment(),
          c.mode(),
          c.isolation(),
          c.requests(),
          c.initialValue(),
          c.delayMs(),
          c.interleaving(),
          run.status(),
          run.createdAt(),
          run.completedAt(),
          r == null ? null : r.invariant().holds(),
          m == null ? null : m.succeeded(),
          m == null ? null : m.rejected(),
          m == null ? null : m.aborted(),
          m == null ? null : m.conflicts(),
          m == null ? null : m.lockWaits(),
          m == null ? null : m.p95Micros(),
          run.traceId());
    }
  }

  /** What the lab is doing: the run in flight, if any, and where its traces can be seen. */
  public record LabState(Long runningRunId, String grafanaUrl) {}

  /** Rows deleted per race_lab table. */
  public record Reset(Map<String, Integer> deleted) {}
}
