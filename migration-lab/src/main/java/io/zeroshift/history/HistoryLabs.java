package io.zeroshift.history;

import io.zeroshift.kafkalab.KafkaLabRuns;
import io.zeroshift.platform.web.ApiException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Runs the events-over-time labs one step at a time, in order. Step results stay in memory for the
 * page; a run that reaches its last step is stored in lab_run with every step's result.
 */
@Component
public class HistoryLabs {
  public static final String LAB = "history";

  public record StepResult(
      int step, String name, Instant at, long millis, boolean ok, JsonNode result, String error) {}

  public record Run(String lab, Instant startedAt, List<StepResult> steps) {}

  public record Info(
      String id, String title, String summary, List<String> steps, List<String> plan, Run run) {}

  private final Map<String, HistoryLab> labs = new LinkedHashMap<>();
  private final Map<String, Run> runs = new LinkedHashMap<>();
  private final Map<String, ObjectNode> memos = new LinkedHashMap<>();
  private final KafkaLabRuns stored;
  private final JsonMapper json = JsonMapper.builder().build();

  public HistoryLabs(List<HistoryLab> all, KafkaLabRuns stored) {
    this.stored = stored;
    // Page order: time travel, projections, schemas, compaction.
    for (var id : List.of("time-travel", "projection", "schema", "compaction"))
      all.stream().filter(l -> l.id().equals(id)).findFirst().ifPresent(l -> labs.put(id, l));
  }

  public synchronized List<Info> all() {
    return labs.values().stream()
        .map(
            l ->
                new Info(
                    l.id(), l.title(), l.summary(), HistoryLab.STEPS, l.plan(), runs.get(l.id())))
        .toList();
  }

  /**
   * Runs step {@code n} of lab {@code id}: the next one, or step 0 to start over. {@code orderId}
   * picks the order for step 0 where the lab uses one.
   */
  public Run step(String id, int n, String orderId) {
    var lab = labs.get(id);
    if (lab == null) throw new ApiException(HttpStatus.NOT_FOUND, "UNKNOWN_LAB", "No lab " + id);
    ObjectNode memo;
    synchronized (this) {
      var run = runs.get(id);
      int next = run == null ? 0 : run.steps().size();
      if (n != 0 && n != next)
        throw new ApiException(
            HttpStatus.CONFLICT,
            "OUT_OF_ORDER",
            "Run the steps in order: step " + (next + 1) + " is next");
      if (n == 0) {
        memos.put(id, json.createObjectNode());
        if (orderId != null && !orderId.isBlank()) memos.get(id).put("orderId", orderId);
        runs.put(id, new Run(id, Instant.now(), List.of()));
      }
      memo = memos.get(id);
    }
    // Steps can take a minute (waiting for sagas or the log cleaner): not under the lock.
    long t0 = System.nanoTime();
    var at = Instant.now();
    StepResult result;
    try {
      var output = lab.run(n, memo);
      result = new StepResult(n, HistoryLab.STEPS.get(n), at, ms(t0), true, output, null);
    } catch (Exception e) {
      result =
          new StepResult(
              n,
              HistoryLab.STEPS.get(n),
              at,
              ms(t0),
              false,
              null,
              e.getClass().getSimpleName() + ": " + e.getMessage());
    }
    synchronized (this) {
      var run = runs.get(id);
      var steps = new ArrayList<>(run.steps());
      steps.add(result);
      var updated = new Run(id, run.startedAt(), List.copyOf(steps));
      // A failed step can be retried: it is not kept as done.
      if (!result.ok()) {
        runs.put(id, new Run(id, run.startedAt(), run.steps()));
        throw new ApiException(HttpStatus.BAD_GATEWAY, "STEP_FAILED", result.error());
      }
      runs.put(id, updated);
      if (steps.size() == HistoryLab.STEPS.size())
        stored.record(LAB, id, lab.title() + ": all six steps completed", updated);
      return updated;
    }
  }

  private static long ms(long t0) {
    return (System.nanoTime() - t0) / 1_000_000;
  }
}
