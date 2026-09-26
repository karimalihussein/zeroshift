package io.zeroshift.racelab.application;

import io.zeroshift.racelab.application.Experiment.RunContext;
import io.zeroshift.racelab.application.Participant.Aborted;
import io.zeroshift.racelab.application.port.LabDatabase;
import io.zeroshift.racelab.application.port.RunRepository;
import io.zeroshift.racelab.application.port.Tracing;
import io.zeroshift.racelab.domain.EventType;
import io.zeroshift.racelab.domain.Interleaving;
import io.zeroshift.racelab.domain.Isolation;
import io.zeroshift.racelab.domain.Mode;
import io.zeroshift.racelab.domain.RaceEvent;
import io.zeroshift.racelab.domain.RaceLabErrors;
import io.zeroshift.racelab.domain.Run;
import io.zeroshift.racelab.domain.RunConfig;
import io.zeroshift.racelab.domain.RunResult;
import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Creates and runs experiments. A run seeds its own rows, starts one thread and one connection per
 * request, lets them race through the experiment's script, then checks the invariant against the
 * committed rows and derives the metrics and explanation from the recorded events. One run at a
 * time: two concurrent runs would contend with each other and blur what each one shows.
 */
public class ExperimentEngine implements AutoCloseable {
  static final long RUN_LIMIT_SECONDS = 90;
  private static final String LANES = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  private final Experiments experiments;
  private final LabDatabase db;
  private final RunRepository runs;
  private final Tracing tracing;
  private final RunStreams streams;
  private final ExecutorService executor =
      Executors.newSingleThreadExecutor(
          Thread.ofPlatform().name("race-lab-engine").daemon().factory());
  private final AtomicLong running = new AtomicLong();
  private final String instance = hostName();

  public ExperimentEngine(
      Experiments experiments,
      LabDatabase db,
      RunRepository runs,
      Tracing tracing,
      RunStreams streams) {
    this.experiments = experiments;
    this.db = db;
    this.runs = runs;
    this.tracing = tracing;
    this.streams = streams;
  }

  /** Validates the configuration, fills in the experiment's defaults and records a new run. */
  public Run create(RunConfig requested) {
    var experiment = experiments.get(requested.experiment());
    var config = normalize(experiment, requested);
    var requests = new ArrayList<Run.Request>();
    for (int i = 0; i < config.requests(); i++)
      requests.add(
          new Run.Request(
              String.valueOf(LANES.charAt(i)),
              id("req"),
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              experiment.role(i)));
    return runs.create(config, requests);
  }

  /** Starts a created run in the background; its events stream as they happen. */
  public Run start(long id) {
    var run = runs.find(id).orElseThrow(() -> new RaceLabErrors.RunNotFound(id));
    if (run.status() != Run.Status.CREATED)
      throw new RaceLabErrors.RunAlreadyStarted(id, run.status());
    if (!running.compareAndSet(0, id)) throw new RaceLabErrors.LabBusy(running.get());
    var started = run.withStatus(Run.Status.RUNNING, Instant.now(), null, null);
    runs.save(started);
    executor.submit(tracing.propagate(() -> execute(started)));
    return started;
  }

  /**
   * Deletes every run, event and scenario row of the lab (race_lab only) and restarts their ids, so
   * the next run starts from the same state as the first one ever did. Refused while a run
   * executes.
   */
  public Map<String, Integer> reset() {
    if (!running.compareAndSet(0, -1)) throw new RaceLabErrors.LabBusy(running.get());
    try {
      var deleted = new LinkedHashMap<String, Integer>(runs.deleteAll());
      deleted.putAll(db.resetScenarios());
      return deleted;
    } finally {
      running.set(0);
    }
  }

  /** The run currently executing, or 0. */
  public long running() {
    return running.get();
  }

  public Experiments experiments() {
    return experiments;
  }

  static RunConfig normalize(Experiment experiment, RunConfig c) {
    var info = experiment.info();
    var limits = info.limits();
    var mode = c.mode() == null ? info.defaultMode() : c.mode();
    var modeInfo = info.mode(mode);
    if (modeInfo == null)
      throw new RaceLabErrors.InvalidConfig(
          info.title()
              + " does not support "
              + mode.label()
              + "; it supports "
              + info.modes().stream().map(m -> m.mode().label()).toList());
    // Unset values arrive as negatives (Integer.MIN_VALUE for the initial value): defaults apply.
    int requests = c.requests() <= 0 ? limits.defaultRequests() : c.requests();
    if (requests < limits.minRequests() || requests > limits.maxRequests())
      throw new RaceLabErrors.InvalidConfig(
          info.title()
              + " runs "
              + limits.minRequests()
              + "–"
              + limits.maxRequests()
              + " requests");
    int value = c.initialValue() == Integer.MIN_VALUE ? limits.defaultValue() : c.initialValue();
    if (value < limits.minValue() || value > limits.maxValue())
      throw new RaceLabErrors.InvalidConfig(
          info.valueLabel() + " must be " + limits.minValue() + "–" + limits.maxValue());
    int delay = c.delayMs() < 0 ? limits.defaultDelayMs() : c.delayMs();
    if (delay > 5000) throw new RaceLabErrors.InvalidConfig("The delay must be 0–5000 ms");
    int retries =
        c.maxRetries() < 0
            ? (experiment.retries(mode) ? limits.defaultRetries() : 0)
            : c.maxRetries();
    if (retries > 10) throw new RaceLabErrors.InvalidConfig("Retries must be 0–10");
    Isolation isolation =
        mode == Mode.SERIALIZABLE
            ? Isolation.SERIALIZABLE
            : c.isolation() != null ? c.isolation() : modeInfo.isolation();
    return new RunConfig(
        info.id(),
        mode,
        isolation,
        requests,
        value,
        delay,
        c.interleaving() == null ? Interleaving.CONTROLLED : c.interleaving(),
        retries);
  }

  // ---- a run -----------------------------------------------------------------------------------

  private void execute(Run run) {
    var experiment = experiments.get(run.experiment());
    var config = run.config();
    var participants = new ArrayList<TransactionParticipant>();
    Run current = run;
    try (var span =
            tracing.start(
                "race-lab run " + run.id(),
                Map.of(
                    "race.run", String.valueOf(run.id()),
                    "race.experiment", run.experiment(),
                    "race.mode", config.mode().name(),
                    "db.isolation", config.isolation().sql(),
                    "race.requests", String.valueOf(config.requests())));
        var recorder = new EventRecorder(run.id(), runs, streams);
        var monitor = new LockMonitor(db, run.id())) {
      current = run.withStatus(Run.Status.RUNNING, run.startedAt(), null, span.traceId());
      runs.save(current);
      streams.running(run.id(), recorder::events);
      var keys = experiment.seed(db, new RunContext(run.id(), config, run.requests(), Map.of()));
      var context = new RunContext(run.id(), config, run.requests(), keys);
      var initial = experiment.observe(db, context);
      var observer = new StateObserver(experiment, context, recorder, db, initial);
      recorder.record(started(experiment, config, span, keys));
      observer.record("initial state", null);

      var choreography =
          new Choreography(config.requests(), config.interleaving() == Interleaving.CONTROLLED);
      var backends = new ConcurrentHashMap<Integer, String>();
      for (int i = 0; i < config.requests(); i++)
        participants.add(
            new TransactionParticipant(
                context,
                i,
                config.isolation(),
                recorder,
                choreography,
                tracing,
                db,
                monitor,
                observer::afterTransaction,
                instance,
                backends));
      var go = new CountDownLatch(1);
      var threads = new ArrayList<Thread>();
      for (var p : participants) {
        Runnable request = () -> request(experiment, p, recorder, choreography, config, go);
        threads.add(
            Thread.ofPlatform()
                .name("race-" + run.id() + "-" + p.lane())
                .start(tracing.propagate(request)));
      }
      go.countDown();
      if (!joinAll(threads, TimeUnit.SECONDS.toNanos(RUN_LIMIT_SECONDS))) {
        backends.keySet().forEach(db::cancel);
        joinAll(threads, TimeUnit.SECONDS.toNanos(10));
        throw new IllegalStateException(
            "Run exceeded " + RUN_LIMIT_SECONDS + " s and was cancelled");
      }

      var result = participants.stream().map(TransactionParticipant::result).toList();
      var finalState = experiment.observe(db, context);
      observer.settled(finalState);
      var invariant = experiment.check(context, initial, finalState, result, recorder.events());
      recorder.record(
          RaceEvent.of(EventType.INVARIANT_CHECKED, "LAB")
              .ok(invariant.holds())
              .target(invariant.statement())
              .valueRead(invariant.expected())
              .valueWritten(invariant.actual())
              .message(invariant.detail())
              .trace(span.traceId(), span.spanId()));
      var events = recorder.events();
      var metrics = RunAnalysis.metrics(events, result);
      var highlights = RunAnalysis.highlights(events, invariant);
      var explanation =
          RunAnalysis.explain(
              events,
              invariant,
              experiment.conclusion(context, invariant, result),
              experiment.mechanism(config),
              experiment.fix(config, invariant));
      recorder.record(
          RaceEvent.of(EventType.EXPERIMENT_COMPLETED, "LAB")
              .ok(invariant.holds())
              .message(explanation.headline())
              .data(
                  Map.of(
                      "succeeded", metrics.succeeded(),
                      "rejected", metrics.rejected(),
                      "aborted", metrics.aborted()))
              .trace(span.traceId(), span.spanId()));
      if (!invariant.holds()) span.error(invariant.actual());
      recorder.close();
      current =
          current.completed(
              new RunResult(
                  metrics, invariant, result, initial, finalState, highlights, explanation),
              Instant.now());
      runs.save(current);
    } catch (RuntimeException | Error e) {
      current =
          current.failed(
              Objects.toString(e.getMessage(), e.getClass().getSimpleName()), Instant.now());
      runs.save(current);
    } finally {
      participants.forEach(TransactionParticipant::close);
      running.set(0);
      streams.finished(current);
    }
  }

  private void request(
      Experiment experiment,
      TransactionParticipant p,
      EventRecorder recorder,
      Choreography choreography,
      RunConfig config,
      CountDownLatch go) {
    try {
      go.await();
      for (int attempt = 1; ; attempt++) {
        p.startAttempt(attempt);
        try {
          experiment.attempt(p);
          if (!p.finished())
            p.failed(new IllegalStateException("the attempt ended without commit or reject"));
          break;
        } catch (Aborted a) {
          if (attempt > config.maxRetries()) {
            p.aborted(a);
            break;
          }
          recorder.record(
              p.event(EventType.RETRY_SCHEDULED)
                  .message(
                      "attempt "
                          + (attempt + 1)
                          + " of "
                          + (config.maxRetries() + 1)
                          + " after "
                          + a.why().name().toLowerCase().replace('_', ' ')));
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      p.failed(e);
    } catch (RuntimeException e) {
      p.failed(e);
    } finally {
      var r = p.result();
      recorder.record(
          p.event(EventType.REQUEST_COMPLETED)
              .outcome(r.outcome().name())
              .durationMicros(r.latencyMicros())
              .message(r.detail())
              .data(
                  Map.of(
                      "attempts", r.attempts(),
                      "lockWaitMicros", r.lockWaitMicros(),
                      "syncWaitMicros", r.syncWaitMicros())));
      choreography.done(p.index());
    }
  }

  private RaceEvent.Builder started(
      Experiment experiment, RunConfig config, Tracing.TraceSpan span, Map<String, Object> keys) {
    var data = new LinkedHashMap<String, Object>();
    data.put("mode", config.mode().name());
    data.put("isolation", config.isolation().sql());
    data.put("requests", config.requests());
    data.put("initialValue", config.initialValue());
    data.put("delayMs", config.delayMs());
    data.put("interleaving", config.interleaving().name());
    data.put("maxRetries", config.maxRetries());
    data.put("deadlockTimeout", safeSetting("deadlock_timeout"));
    data.put("rows", keys);
    return RaceEvent.of(EventType.EXPERIMENT_STARTED, "LAB")
        .target(experiment.info().invariant())
        .message(experiment.info().title())
        .isolation(config.isolation().sql())
        .data(data)
        .trace(span.traceId(), span.spanId());
  }

  private String safeSetting(String name) {
    try {
      return db.setting(name);
    } catch (RuntimeException e) {
      return "unknown";
    }
  }

  /** Records the committed state (the database lane) whenever it changes. */
  private static final class StateObserver {
    private final Experiment experiment;
    private final RunContext context;
    private final EventRecorder recorder;
    private final LabDatabase db;
    private String last;
    private Map<String, Object> state;

    StateObserver(
        Experiment experiment,
        RunContext context,
        EventRecorder recorder,
        LabDatabase db,
        Map<String, Object> initial) {
      this.experiment = experiment;
      this.context = context;
      this.recorder = recorder;
      this.db = db;
      this.state = initial;
    }

    void afterTransaction(String lane) {
      record("after " + lane + " ended", lane);
    }

    /** The state after every request finished, as the invariant check read it. */
    synchronized void settled(Map<String, Object> finalState) {
      state = finalState;
      record("final state", null);
    }

    synchronized void record(String why, String lane) {
      try {
        if (lane != null) state = experiment.observe(db, context);
        var shown = experiment.describe(state);
        if (shown.equals(last)) return;
        last = shown;
        recorder.record(
            RaceEvent.of(EventType.STATE_OBSERVED, "DB")
                .valueWritten(shown)
                .message(lane == null ? why : "committed state " + why)
                .target(lane)
                .data(state));
      } catch (RuntimeException e) {
        // Observation is for the database lane only; the run itself goes on.
      }
    }
  }

  private static boolean joinAll(List<Thread> threads, long nanos) {
    long deadline = System.nanoTime() + nanos;
    for (var t : threads) {
      long left = deadline - System.nanoTime();
      try {
        if (left <= 0 || !t.join(java.time.Duration.ofNanos(left))) return false;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return true;
  }

  private static String id(String prefix) {
    var bytes = new byte[5];
    ThreadLocalRandom.current().nextBytes(bytes);
    return prefix + "-" + HexFormat.of().formatHex(bytes);
  }

  private static String hostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return "unknown";
    }
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}
