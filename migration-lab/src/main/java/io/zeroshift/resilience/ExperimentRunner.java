package io.zeroshift.resilience;

import io.zeroshift.kafkalab.KafkaLabRuns;
import io.zeroshift.platform.web.ApiException;
import io.zeroshift.resilience.Experiment.Phase;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Runs one experiment at a time, step by step. Interactive runs stop after every step and wait for
 * the operator; automatic runs (the drills) go straight through. A step runs its action, lets the
 * system settle, then re-evaluates its checks against each new sample until every one holds or its
 * deadline passes. A failed check is a result, not an abort: the run continues to recover and
 * verify, so the lab is never left broken. Finished runs are stored in lab_run.
 */
@Component
public class ExperimentRunner {
  public static final String LAB = "resilience";
  private static final Logger log = LoggerFactory.getLogger(ExperimentRunner.class);

  public record CheckResult(String claim, int withinSeconds, String status, String observed) {}

  public record StepResult(
      Phase phase,
      String title,
      String text,
      String status,
      Instant startedAt,
      Instant finishedAt,
      String did,
      List<CheckResult> checks) {}

  /**
   * @param status running, waiting (for the operator), passed, failed or aborted
   * @param current index of the step running or last run
   */
  public record Run(
      String id,
      String experiment,
      String title,
      boolean auto,
      String status,
      int current,
      Instant startedAt,
      Instant finishedAt,
      List<StepResult> steps) {}

  public record Marker(long t, String kind, String label) {}

  private final ExperimentCatalog catalog;
  private final Sampler sampler;
  private final Levers levers;
  private final LoadGenerator load;
  private final KafkaLabRuns runs;
  private final List<Marker> markers = new CopyOnWriteArrayList<>();

  private volatile Session session;

  public ExperimentRunner(
      ExperimentCatalog catalog,
      Sampler sampler,
      Levers levers,
      LoadGenerator load,
      KafkaLabRuns runs) {
    this.catalog = catalog;
    this.sampler = sampler;
    this.levers = levers;
    this.load = load;
    this.runs = runs;
    sampler.keepSamplingWhile(() -> session != null && session.active());
  }

  public Run current() {
    var s = session;
    return s == null ? null : s.view();
  }

  public List<Marker> markers(long since) {
    return markers.stream().filter(m -> m.t() > since).toList();
  }

  public void mark(String kind, String label) {
    markers.add(new Marker(System.currentTimeMillis(), kind, label));
    while (markers.size() > 200) markers.removeFirst();
  }

  public synchronized Run start(String id, boolean auto) {
    var experiment = catalog.get(id);
    if (experiment == null)
      throw new ApiException(HttpStatus.NOT_FOUND, "UNKNOWN_EXPERIMENT", "No experiment " + id);
    if (session != null && session.active())
      throw new ApiException(
          HttpStatus.CONFLICT,
          "EXPERIMENT_RUNNING",
          session.experiment.title() + " is running; finish or abort it first");
    if (experiment.needsChaos() && !levers.chaos().configured())
      throw new ApiException(
          HttpStatus.CONFLICT,
          "CHAOS_NOT_RUNNING",
          "This experiment breaks a network link and needs Toxiproxy: docker compose -f"
              + " docker-compose.yml -f docker-compose.override.yml -f docker-compose.chaos.yml up -d");
    session = new Session(experiment, auto);
    session.advance();
    return session.view();
  }

  /** Runs the next step of an interactive run; with {@code auto}, every remaining step. */
  public synchronized Run next(boolean auto) {
    var s = session;
    if (s == null || !s.waiting())
      throw new ApiException(
          HttpStatus.CONFLICT, "NOT_WAITING", "No experiment is waiting for its next step");
    if (auto) s.auto = true;
    s.advance();
    return s.view();
  }

  /** Stops after the current step and puts the whole lab back to defaults. */
  public synchronized Run abort() {
    var s = session;
    if (s == null)
      throw new ApiException(HttpStatus.CONFLICT, "NO_EXPERIMENT", "No experiment has run");
    s.abort();
    return s.view();
  }

  private final class Session {
    final String id = UUID.randomUUID().toString().substring(0, 8);
    final Experiment experiment;
    final Instant startedAt = Instant.now();
    final List<StepResult> results = new CopyOnWriteArrayList<>();
    final EnumMap<Phase, List<Sample>> byPhase = new EnumMap<>(Phase.class);
    volatile boolean auto;
    volatile String status = "running";
    volatile int current = -1;
    volatile Instant finishedAt;
    volatile boolean aborted;
    Thread worker;

    Session(Experiment experiment, boolean auto) {
      this.experiment = experiment;
      this.auto = auto;
      for (var step : experiment.steps())
        results.add(
            new StepResult(
                step.phase(),
                step.title(),
                step.text(),
                "pending",
                null,
                null,
                null,
                checks(step, "pending")));
    }

    boolean active() {
      return status.equals("running") || status.equals("waiting");
    }

    boolean waiting() {
      return status.equals("waiting");
    }

    void advance() {
      status = "running";
      worker = Thread.ofVirtual().name("experiment-" + experiment.id()).start(this::runSteps);
    }

    void runSteps() {
      try {
        do {
          current++;
          runStep(current);
          if (aborted) return;
        } while (current < experiment.steps().size() - 1 && auto);
        if (current < experiment.steps().size() - 1) status = "waiting";
        else finish();
      } catch (RuntimeException e) {
        log.warn("experiment {} failed", experiment.id(), e);
        status = "failed";
        cleanUp("error: " + e.getMessage());
      }
    }

    void runStep(int index) {
      var step = experiment.steps().get(index);
      var startedAt = Instant.now();
      long from = startedAt.toEpochMilli();
      update(index, "running", startedAt, null, null, checks(step, "pending"));
      String did = null;
      if (step.action() != null) {
        try {
          did = step.action().run();
        } catch (Exception e) {
          did = "failed: " + e.getMessage();
          update(index, "failed", startedAt, Instant.now(), did, checks(step, "not run"));
          mark(kind(step.phase()), step.title() + " (failed)");
          return;
        }
        mark(kind(step.phase()), step.title());
      }
      update(index, "running", startedAt, null, did, checks(step, "pending"));
      sleepSeconds(step.settleSeconds());
      var outcomes = new ArrayList<CheckResult>();
      for (var c : step.checks())
        outcomes.add(new CheckResult(c.claim(), c.withinSeconds(), "pending", null));
      boolean allHold = step.checks().isEmpty();
      while (!aborted && !step.checks().isEmpty()) {
        var samples = sampler.since(from);
        byPhase.put(step.phase(), samples);
        var window = new Experiment.Window(samples, byPhase);
        long elapsed = (System.currentTimeMillis() - from) / 1000;
        allHold = true;
        boolean anyPending = false;
        boolean othersDecided = true;
        for (int i = 0; i < step.checks().size(); i++)
          if (!step.checks().get(i).throughout() && outcomes.get(i).status().equals("pending"))
            othersDecided = false;
        for (int i = 0; i < step.checks().size(); i++) {
          var check = step.checks().get(i);
          if (outcomes.get(i).status().equals("held")) continue;
          if (check.throughout()
              && (!othersDecided || elapsed < check.withinSeconds() + step.settleSeconds())) {
            outcomes.set(
                i,
                new CheckResult(
                    check.claim(), check.withinSeconds(), "pending", "judged over the whole step"));
            allHold = false;
            anyPending = true;
            continue;
          }
          var o = check.probe().apply(window);
          boolean expired =
              check.throughout() || elapsed >= check.withinSeconds() + step.settleSeconds();
          var status = o.holds() ? "held" : expired ? "failed" : "pending";
          outcomes.set(
              i, new CheckResult(check.claim(), check.withinSeconds(), status, o.observed()));
          if (!o.holds()) allHold = false;
          if (status.equals("pending")) anyPending = true;
        }
        update(index, "running", startedAt, null, did, List.copyOf(outcomes));
        if (allHold || !anyPending) break;
        sleepSeconds(1);
      }
      byPhase.put(step.phase(), sampler.since(from));
      update(
          index,
          step.checks().isEmpty() ? "done" : allHold ? "passed" : "failed",
          startedAt,
          Instant.now(),
          did,
          step.checks().isEmpty() ? List.of() : List.copyOf(outcomes));
    }

    void finish() {
      boolean passed = results.stream().noneMatch(r -> r.status().equals("failed"));
      status = passed ? "passed" : "failed";
      cleanUp(passed ? "every claim held" : "some claims did not hold");
    }

    void abort() {
      aborted = true;
      if (worker != null) worker.interrupt();
      status = "aborted";
      cleanUp("aborted by operator");
    }

    /** Stops the load and resets every lever, then stores the run. */
    void cleanUp(String summary) {
      finishedAt = Instant.now();
      load.stop();
      var failures = levers.resetAll();
      mark("reset", "Lab reset");
      try {
        var failed =
            results.stream()
                .flatMap(r -> r.checks().stream())
                .filter(c -> c.status().equals("failed"))
                .map(CheckResult::claim)
                .toList();
        runs.record(
            LAB,
            experiment.id(),
            status
                + ": "
                + summary
                + (failed.isEmpty() ? "" : " — not held: " + String.join("; ", failed))
                + (failures.isEmpty() ? "" : " — reset problems: " + failures),
            view());
      } catch (RuntimeException e) {
        log.warn("could not store the {} run", experiment.id(), e);
      }
    }

    void update(
        int index,
        String status,
        Instant startedAt,
        Instant finishedAt,
        String did,
        List<CheckResult> checks) {
      var step = experiment.steps().get(index);
      results.set(
          index,
          new StepResult(
              step.phase(), step.title(), step.text(), status, startedAt, finishedAt, did, checks));
    }

    Run view() {
      return new Run(
          id,
          experiment.id(),
          experiment.title(),
          auto,
          status,
          current,
          startedAt,
          finishedAt,
          List.copyOf(results));
    }

    private void sleepSeconds(int seconds) {
      try {
        Thread.sleep(seconds * 1000L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static List<CheckResult> checks(Experiment.Step step, String status) {
    return step.checks().stream()
        .map(c -> new CheckResult(c.claim(), c.withinSeconds(), status, null))
        .toList();
  }

  private static String kind(Phase phase) {
    return switch (phase) {
      case INJECT -> "inject";
      case MITIGATE -> "mitigate";
      case RECOVER -> "recover";
      default -> "note";
    };
  }
}
