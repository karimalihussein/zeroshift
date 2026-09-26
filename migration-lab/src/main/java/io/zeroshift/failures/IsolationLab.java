package io.zeroshift.failures;

import static io.zeroshift.failures.Checks.check;
import static io.zeroshift.failures.Checks.compare;

import io.zeroshift.racelab.application.ExperimentEngine;
import io.zeroshift.racelab.application.port.RunRepository;
import io.zeroshift.racelab.domain.Interleaving;
import io.zeroshift.racelab.domain.Isolation;
import io.zeroshift.racelab.domain.Mode;
import io.zeroshift.racelab.domain.RaceLabErrors;
import io.zeroshift.racelab.domain.Run;
import io.zeroshift.racelab.domain.RunConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * The same concurrency scenario under each strategy, compared. Every run is the race lab's engine:
 * one thread and one PostgreSQL connection per request, controlled interleaving so the dangerous
 * schedule happens every time, the invariant checked on committed rows, lock waits and aborts as
 * PostgreSQL reported them. Each run stays in the race lab, where its timeline can be opened.
 */
@Component
public class IsolationLab implements FailureLab {
  static final String LOST_UPDATE = "lost-update";
  static final String WRITE_SKEW = "write-skew";
  static final String READ_TWICE = "non-repeatable-read";
  static final int DEPOSITS = 6;

  private final ExperimentEngine engine;
  private final RunRepository runs;
  private final JsonMapper json = JsonMapper.builder().findAndAddModules().build();

  public IsolationLab(ExperimentEngine engine, RunRepository runs) {
    this.engine = engine;
    this.runs = runs;
  }

  @Override
  public String id() {
    return "isolation";
  }

  @Override
  public String title() {
    return "Isolation anomalies";
  }

  @Override
  public String summary() {
    return DEPOSITS
        + " concurrent deposits into one balance (lost update) and two doctors leaving"
        + " the same night shift (write skew), each run under every strategy on real PostgreSQL"
        + " transactions and compared side by side.";
  }

  @Override
  public String naive() {
    return "Read, decide in the application, write back, at PostgreSQL's default READ COMMITTED:"
        + " every statement sees the latest committed data, and nothing protects the gap between"
        + " a read and the write that depends on it.";
  }

  @Override
  public String correct() {
    return "Protect the decision: a version check (optimistic locking) or SERIALIZABLE, each with"
        + " a retry loop; or an atomic update or row locks where they cover everything the decision"
        + " read.";
  }

  @Override
  public List<String> plan() {
    return List.of(
        "READ COMMITTED as it is: a report reads a balance twice in one transaction while a deposit commits in between. Its two reads differ, by design: every statement takes a fresh snapshot.",
        "Run the naive read-modify-write at READ COMMITTED: "
            + DEPOSITS
            + " deposits of 10 read the balance together and write their sums. Run the doctors' check-then-leave at READ COMMITTED and at REPEATABLE READ.",
        "Read what happened from the runs' own events: every request committed and none waited, yet money is missing from the balance and nobody is on call. Snapshot isolation (REPEATABLE READ) did not prevent write skew.",
        "Run the same scenarios with the correct designs: optimistic locking with retries, SERIALIZABLE with retries, an atomic update; for the doctors SERIALIZABLE with retries and locking every row the decision read.",
        "Show why the retry loop is part of the design: SERIALIZABLE with no retries keeps the balance right but turns conflicts into errors for users. Then compare every run: invariant, commits, aborts, conflicts, retries, lock waits and latency.");
  }

  @Override
  public List<String> requires() {
    return List.of("the race lab's engine (schema race_lab of the control plane's PostgreSQL)");
  }

  @Override
  public ObjectNode run(int stage, ObjectNode memo, Trace trace) throws Exception {
    var result = json.createObjectNode();
    var ids = memo.has("runs") ? (ArrayNode) memo.get("runs") : memo.putArray("runs");
    switch (stage) {
      case 0 -> {
        var rc = race(READ_TWICE, Mode.UNSAFE, Isolation.READ_COMMITTED, -1, -1, trace);
        var rr = race(READ_TWICE, Mode.UNSAFE, Isolation.REPEATABLE_READ, -1, -1, trace);
        result.set("readCommitted", summary(rc));
        result.set("repeatableRead", summary(rr));
        result.set("naiveSql", json.valueToTree(sql(LOST_UPDATE, Mode.UNSAFE)));
        check(
            result, "READ COMMITTED: the two reads differ", false, rc.result().invariant().holds());
        check(
            result, "REPEATABLE READ: the two reads agree", true, rr.result().invariant().holds());
      }
      case 1 -> {
        var lost = race(LOST_UPDATE, Mode.UNSAFE, Isolation.READ_COMMITTED, DEPOSITS, 0, trace);
        var skewRc = race(WRITE_SKEW, Mode.UNSAFE, Isolation.READ_COMMITTED, -1, 0, trace);
        var skewRr = race(WRITE_SKEW, Mode.UNSAFE, Isolation.REPEATABLE_READ, -1, 0, trace);
        for (var r : List.of(lost, skewRc, skewRr)) ids.add(r.id());
        memo.put("naiveLost", lost.id());
        memo.put("naiveSkew", skewRr.id());
        result.set("lostUpdate", summary(lost));
        result.set("writeSkewReadCommitted", summary(skewRc));
        result.set("writeSkewRepeatableRead", summary(skewRr));
        check(
            result,
            "lost update at READ COMMITTED breaks the balance",
            false,
            lost.result().invariant().holds());
        check(
            result,
            "write skew at READ COMMITTED leaves nobody on call",
            false,
            skewRc.result().invariant().holds());
        check(
            result,
            "write skew at REPEATABLE READ leaves nobody on call",
            false,
            skewRr.result().invariant().holds());
      }
      case 2 -> {
        var lost = find(memo.path("naiveLost").asLong());
        var skew = find(memo.path("naiveSkew").asLong());
        var m = lost.result().metrics();
        result.set("lostUpdate", evidence(lost));
        result.set("writeSkew", evidence(skew));
        check(result, "deposits that committed", DEPOSITS, m.succeeded());
        check(result, "errors or aborts any user saw", 0, m.aborted() + m.conflicts());
        // The writers did queue on the row lock (READ COMMITTED re-reads the row before updating
        // it),
        // but each then wrote the value it had computed from its stale read: no conflict detected.
        result.put("lockWaits", m.lockWaits());
        check(
            result,
            "conflicts the database detected",
            0,
            m.versionConflicts() + m.serializationFailures() + m.deadlocks());
        check(
            result, "balance matches committed deposits", false, lost.result().invariant().holds());
        compare(
            result,
            "Lost update: balance",
            lost.result().invariant().actual()
                + " (expected "
                + lost.result().invariant().expected()
                + ")",
            null);
        compare(result, "Write skew: doctors on call", skew.result().invariant().actual(), null);
        compare(result, "What users saw", "every request succeeded; no error anywhere", null);
        compare(
            result,
            "Row lock waits",
            m.lockWaits() + " (the writers queued, then overwrote each other anyway)",
            null);
      }
      case 3 -> {
        var fixes = new ArrayList<Run>();
        fixes.add(race(LOST_UPDATE, Mode.OPTIMISTIC, null, DEPOSITS, DEPOSITS, trace));
        fixes.add(race(LOST_UPDATE, Mode.SERIALIZABLE, null, DEPOSITS, DEPOSITS, trace));
        fixes.add(race(LOST_UPDATE, Mode.ATOMIC, null, DEPOSITS, 0, trace));
        fixes.add(race(WRITE_SKEW, Mode.SERIALIZABLE, null, -1, 3, trace));
        fixes.add(race(WRITE_SKEW, Mode.PESSIMISTIC, null, -1, 0, trace));
        var out = result.putArray("runs");
        for (var r : fixes) {
          ids.add(r.id());
          out.add(summary(r));
          check(result, label(r) + " keeps the invariant", true, r.result().invariant().holds());
        }
        var optimistic = fixes.get(0).result().metrics();
        var serializable = fixes.get(1).result().metrics();
        check(
            result,
            "optimistic: every deposit committed after retries",
            DEPOSITS,
            optimistic.succeeded());
        check(
            result,
            "optimistic: stale versions rejected",
            "at least 1",
            optimistic.versionConflicts(),
            optimistic.versionConflicts() > 0);
        check(
            result,
            "serializable: 40001 aborts, then retries",
            "at least 1",
            serializable.serializationFailures(),
            serializable.serializationFailures() > 0);
        check(
            result,
            "serializable: every deposit committed after retries",
            DEPOSITS,
            serializable.succeeded());
        memo.put("serializable", fixes.get(1).id());
        compare(
            result,
            "Lost update: balance",
            null,
            "right under optimistic, SERIALIZABLE and atomic update");
        compare(
            result,
            "Write skew: doctors on call",
            null,
            fixes.get(3).result().invariant().actual()
                + " (SERIALIZABLE aborted one; its retry saw the change)");
        compare(
            result,
            "What users saw",
            null,
            "conflicts, retried inside the service: "
                + optimistic.retries()
                + " (optimistic) and "
                + serializable.retries()
                + " (SERIALIZABLE) retries");
      }
      case 4 -> {
        var noRetry = race(LOST_UPDATE, Mode.SERIALIZABLE, null, DEPOSITS, 0, trace);
        ids.add(noRetry.id());
        var m = noRetry.result().metrics();
        result.set("serializableWithoutRetry", summary(noRetry));
        check(
            result,
            "SERIALIZABLE without retry keeps the balance right",
            true,
            noRetry.result().invariant().holds());
        check(
            result,
            "but conflicting deposits fail: aborted requests",
            "at least 1",
            m.aborted(),
            m.aborted() > 0);
        var matrix = result.putArray("matrix");
        for (var id : ids) matrix.add(json.valueToTree(row(find(id.asLong()))));
        var withRetry = find(memo.path("serializable").asLong()).result().metrics();
        compare(
            result,
            "Conflicts",
            "invisible: last writer wins",
            "detected: "
                + withRetry.serializationFailures()
                + " × 40001 retried; without a retry loop "
                + m.aborted()
                + " of "
                + DEPOSITS
                + " deposits fail");
        compare(
            result,
            "Recovery",
            "none possible from the data: the overwritten deposits leave no trace in the balance",
            "automatic: the service retries the whole transaction on 40001 or a stale version");
      }
      default -> throw new IllegalArgumentException("No stage " + stage);
    }
    return result;
  }

  // ---- race runs
  // -----------------------------------------------------------------------------------

  /** Creates and runs one race-lab run and waits for it; waits its turn if a run is in progress. */
  private Run race(
      String experiment, Mode mode, Isolation isolation, int requests, int retries, Trace trace)
      throws Exception {
    var config =
        new RunConfig(
            experiment,
            mode,
            isolation,
            requests,
            Integer.MIN_VALUE,
            -1,
            Interleaving.CONTROLLED,
            retries);
    long deadline = System.currentTimeMillis() + 120_000;
    Run created = engine.create(config);
    while (true) {
      try {
        engine.start(created.id());
        break;
      } catch (RaceLabErrors.LabBusy busy) {
        if (System.currentTimeMillis() > deadline)
          throw new IllegalStateException("The race lab stayed busy for two minutes");
        Thread.sleep(500);
      }
    }
    while (true) {
      var run = runs.find(created.id()).orElseThrow();
      if (run.status() == Run.Status.COMPLETED) {
        trace.event(
            "race run",
            Map.of(
                "run",
                String.valueOf(run.id()),
                "experiment",
                experiment,
                "mode",
                mode.name(),
                "invariant",
                String.valueOf(run.result().invariant().holds())));
        return run;
      }
      if (run.status() == Run.Status.FAILED)
        throw new IllegalStateException("Race run " + run.id() + " failed: " + run.error());
      if (System.currentTimeMillis() > deadline)
        throw new IllegalStateException("Race run " + run.id() + " did not finish");
      Thread.sleep(150);
    }
  }

  private Run find(long id) {
    return runs.find(id)
        .orElseThrow(
            () -> new IllegalStateException("Race run " + id + " is gone (race lab reset?)"));
  }

  private ObjectNode summary(Run run) {
    var r = run.result();
    var out = json.createObjectNode();
    out.put("raceRun", run.id());
    out.put("strategy", label(run));
    out.put("isolation", run.config().isolation().sql());
    out.put("invariant", r.invariant().statement());
    out.put("holds", r.invariant().holds());
    out.put("expected", r.invariant().expected());
    out.put("actual", r.invariant().actual());
    out.put("detail", r.invariant().detail());
    out.put("traceId", run.traceId());
    out.set(
        "requests",
        json.valueToTree(
            r.requests().stream()
                .map(
                    q ->
                        Map.of(
                            "lane",
                            q.lane(),
                            "outcome",
                            q.outcome().name(),
                            "attempts",
                            q.attempts(),
                            "detail",
                            q.detail() == null ? "" : q.detail()))
                .toList()));
    return out;
  }

  /** The runs' own account of what happened: highlights and the step-by-step explanation. */
  private ObjectNode evidence(Run run) {
    var out = summary(run);
    out.set(
        "keyMoments",
        json.valueToTree(
            run.result().highlights().stream()
                .map(
                    h -> Map.of("title", h.title(), "detail", h.detail() == null ? "" : h.detail()))
                .toList()));
    out.set(
        "why",
        json.valueToTree(
            run.result().explanation().steps().stream()
                .map(s -> s.lane() + ": " + s.text())
                .toList()));
    out.put("conclusion", run.result().explanation().conclusion());
    out.set("finalState", json.valueToTree(run.result().finalState()));
    return out;
  }

  private Map<String, Object> row(Run run) {
    var m = run.result().metrics();
    var row = new LinkedHashMap<String, Object>();
    row.put("scenario", run.experiment());
    row.put("strategy", label(run));
    row.put("isolation", run.config().isolation().sql());
    row.put("retries allowed", run.config().maxRetries());
    row.put("invariant", run.result().invariant().holds());
    row.put("result", run.result().invariant().actual());
    row.put("committed", m.succeeded());
    row.put("rejected", m.rejected());
    row.put("aborted", m.aborted());
    row.put("version conflicts", m.versionConflicts());
    row.put("40001", m.serializationFailures());
    row.put("retries", m.retries());
    row.put("lock wait ms", m.lockWaitMicros() / 1000);
    row.put("p95 ms", m.p95Micros() / 1000);
    row.put("duration ms", m.durationMicros() / 1000);
    row.put("race run", run.id());
    return row;
  }

  private static String label(Run run) {
    var mode = run.config().mode();
    return mode == Mode.UNSAFE ? "Naive read-modify-write" : mode.label();
  }

  private List<String> sql(String experiment, Mode mode) {
    var info = engine.experiments().get(experiment).info();
    var m = info.mode(mode);
    return m == null ? List.of() : List.of(m.sql().split("\n"));
  }

  @Override
  public ObjectNode state() {
    var state = json.createObjectNode();
    state.put("runningRaceRun", engine.running());
    var recent = state.putArray("recentRuns");
    for (var experiment : List.of(LOST_UPDATE, WRITE_SKEW))
      for (var run : runs.recent(experiment, 6))
        if (run.status() == Run.Status.COMPLETED) recent.add(json.valueToTree(row(run)));
    return state;
  }

  @Override
  public ObjectNode reset() {
    // The runs belong to the race lab (its own reset clears them); this lab only forgets its own.
    return json.createObjectNode()
        .put(
            "note",
            "Forgot this lab's run. Its race-lab runs stay in the race lab; /race resets them.");
  }
}
