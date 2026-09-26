package io.zeroshift.failures;

import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.kafkalab.KafkaLabRuns;
import io.zeroshift.platform.web.ApiException;
import io.zeroshift.racelab.application.port.Tracing;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Runs the failure labs one stage at a time, in order, one stage per lab at a time. A stage whose
 * checks do not all hold is kept as the lab's last failure (so its measurements stay visible) but
 * not as done. A run that completes all five stages is stored in lab_run.
 */
@Component
public class FailureLabs {
  public static final String LAB = "failures";

  public record StageResult(
      int stage,
      String name,
      Instant at,
      long millis,
      boolean ok,
      String traceId,
      JsonNode result,
      String error) {}

  public record Run(
      String lab, Instant startedAt, List<StageResult> stages, StageResult lastFailure) {}

  public record Info(
      String id,
      String title,
      String summary,
      String naive,
      String correct,
      List<String> stages,
      List<String> plan,
      List<String> requires,
      boolean busy,
      Run run) {}

  private final Map<String, FailureLab> labs = new LinkedHashMap<>();
  private final Map<String, Run> runs = new LinkedHashMap<>();
  private final Map<String, ObjectNode> memos = new LinkedHashMap<>();
  private final Map<String, Boolean> busy = new ConcurrentHashMap<>();
  private final KafkaLabRuns stored;
  private final Tracing tracing;
  private final MeterRegistry meters;
  private final JsonMapper json = JsonMapper.builder().build();

  public FailureLabs(
      List<FailureLab> all, KafkaLabRuns stored, Tracing tracing, MeterRegistry meters) {
    this.stored = stored;
    this.tracing = tracing;
    this.meters = meters;
    for (var id : List.of("two-phase", "isolation", "disaster-recovery", "trust"))
      all.stream().filter(l -> l.id().equals(id)).findFirst().ifPresent(l -> labs.put(id, l));
  }

  public synchronized List<Info> all() {
    return labs.values().stream().map(this::info).toList();
  }

  private Info info(FailureLab l) {
    return new Info(
        l.id(),
        l.title(),
        l.summary(),
        l.naive(),
        l.correct(),
        FailureLab.STAGES,
        l.plan(),
        l.requires(),
        busy.containsKey(l.id()),
        runs.get(l.id()));
  }

  FailureLab lab(String id) {
    var lab = labs.get(id);
    if (lab == null)
      throw new ApiException(HttpStatus.NOT_FOUND, "UNKNOWN_FAILURE_LAB", "No failure lab " + id);
    return lab;
  }

  /** Runs stage {@code n} of lab {@code id}: the next one, or stage 0 to start over. */
  public Run stage(String id, int n) {
    var lab = lab(id);
    ObjectNode memo;
    synchronized (this) {
      var run = runs.get(id);
      int next = run == null ? 0 : run.stages().size();
      if (n != 0 && n != next)
        throw new ApiException(
            HttpStatus.CONFLICT,
            "STAGE_OUT_OF_ORDER",
            "Run the stages in order: stage " + (next + 1) + " is next");
      claim(id);
      if (n == 0) {
        memos.put(id, json.createObjectNode());
        runs.put(id, new Run(id, Instant.now(), List.of(), null));
      }
      memo = memos.get(id);
    }
    long t0 = System.nanoTime();
    var at = Instant.now();
    StageResult result;
    try (var span =
        tracing.start(
            "failure-lab " + id + " stage " + (n + 1),
            Map.of("failure.lab", id, "failure.stage", FailureLab.STAGES.get(n)))) {
      FailureLab.Trace trace =
          new FailureLab.Trace() {
            @Override
            public void event(String name, Map<String, String> attributes) {
              span.event(name, attributes);
            }

            @Override
            public String traceId() {
              return span.traceId();
            }
          };
      try {
        var output = lab.run(n, memo, trace);
        boolean ok = Checks.allHold(output);
        if (!ok) span.error(Checks.failures(output));
        result =
            new StageResult(
                n,
                FailureLab.STAGES.get(n),
                at,
                ms(t0),
                ok,
                span.traceId(),
                output,
                ok ? null : "A claim did not hold: " + Checks.failures(output));
      } catch (Exception e) {
        span.error(String.valueOf(e.getMessage()));
        result =
            new StageResult(
                n,
                FailureLab.STAGES.get(n),
                at,
                ms(t0),
                false,
                span.traceId(),
                null,
                e.getClass().getSimpleName() + ": " + e.getMessage());
      }
    } finally {
      busy.remove(id);
    }
    meters
        .counter(
            "zeroshift.failure_lab.stages",
            "lab",
            id,
            "stage",
            FailureLab.STAGES.get(n),
            "outcome",
            result.ok() ? "held" : "failed")
        .increment();
    synchronized (this) {
      var run = runs.get(id);
      if (!result.ok()) {
        runs.put(id, new Run(id, run.startedAt(), run.stages(), result));
        throw new ApiException(HttpStatus.BAD_GATEWAY, "STAGE_FAILED", result.error());
      }
      var stages = new ArrayList<>(run.stages());
      stages.add(result);
      var updated = new Run(id, run.startedAt(), List.copyOf(stages), null);
      runs.put(id, updated);
      if (stages.size() == FailureLab.STAGES.size())
        stored.record(LAB, id, lab.title() + ": all five stages held", updated);
      return updated;
    }
  }

  public ObjectNode state(String id) throws Exception {
    return lab(id).state();
  }

  /** Resets one lab's infrastructure and forgets its run. Refused while one of its stages runs. */
  public ObjectNode reset(String id) throws Exception {
    var lab = lab(id);
    synchronized (this) {
      claim(id);
    }
    try {
      var result = lab.reset();
      synchronized (this) {
        runs.remove(id);
        memos.remove(id);
      }
      return result;
    } finally {
      busy.remove(id);
    }
  }

  /** Runs {@code action} while holding the lab, so it cannot interleave with a stage or reset. */
  public <T> T exclusive(String id, java.util.concurrent.Callable<T> action) throws Exception {
    lab(id);
    synchronized (this) {
      claim(id);
    }
    try {
      return action.call();
    } finally {
      busy.remove(id);
    }
  }

  private void claim(String id) {
    if (busy.putIfAbsent(id, Boolean.TRUE) != null)
      throw new ApiException(
          HttpStatus.CONFLICT,
          "FAILURE_LAB_BUSY",
          "A stage of " + id + " is still running; wait for it to finish");
  }

  private static long ms(long t0) {
    return (System.nanoTime() - t0) / 1_000_000;
  }
}
