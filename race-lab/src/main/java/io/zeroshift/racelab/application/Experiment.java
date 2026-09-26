package io.zeroshift.racelab.application;

import io.zeroshift.racelab.application.port.LabDatabase;
import io.zeroshift.racelab.domain.ExperimentInfo;
import io.zeroshift.racelab.domain.Mode;
import io.zeroshift.racelab.domain.RaceEvent;
import io.zeroshift.racelab.domain.Run;
import io.zeroshift.racelab.domain.RunConfig;
import io.zeroshift.racelab.domain.RunResult.Explanation;
import io.zeroshift.racelab.domain.RunResult.InvariantResult;
import io.zeroshift.racelab.domain.RunResult.RequestResult;
import java.util.List;
import java.util.Map;

/**
 * One concurrency lesson: the rows it races on, what each request does per strategy, and the rule a
 * correct execution keeps. The SQL lives here because the SQL is the lesson; the engine runs it,
 * records it and checks the result.
 */
public interface Experiment {
  ExperimentInfo info();

  /** Creates this run's rows (tagged with the run id) and returns their keys. */
  Map<String, Object> seed(LabDatabase db, RunContext run);

  /**
   * One attempt of one request, as one transaction. It ends by {@link Participant#commit} or {@link
   * Participant#reject}; an abort by PostgreSQL or a stale version propagates as {@link
   * Participant.Aborted} and the engine decides about a retry.
   */
  void attempt(Participant p);

  /** The committed state that matters, read on the engine's own connection. */
  Map<String, Object> observe(LabDatabase db, RunContext run);

  /** The database lane's label for a state: "stock=1". */
  String describe(Map<String, Object> state);

  /**
   * Checks the rule against the committed rows ({@code result}); {@code events} are there for rules
   * about what transactions saw (two reads in one transaction), which no row records.
   */
  InvariantResult check(
      RunContext run,
      Map<String, Object> initial,
      Map<String, Object> result,
      List<RequestResult> requests,
      List<RaceEvent> events);

  /** The last sentence of "why": what the captured interleaving did to the data. */
  String conclusion(RunContext run, InvariantResult invariant, List<RequestResult> requests);

  /** How the run's strategy works, as the explanation's "what changed". */
  default String mechanism(RunConfig config) {
    var mode = info().mode(config.mode());
    return mode == null ? "" : mode.how();
  }

  /** The configuration to run next when this one broke the invariant; null when it held. */
  Explanation.Fix fix(RunConfig config, InvariantResult invariant);

  /** What request {@code index} does, when requests differ (reader/writer, ship/cancel). */
  default String role(int index) {
    var roles = info().roles();
    return roles.isEmpty() ? null : roles.get(index % roles.size());
  }

  /** Whether the strategy relies on a retry after an abort (optimistic, serializable). */
  default boolean retries(Mode mode) {
    return mode == Mode.OPTIMISTIC || mode == Mode.SERIALIZABLE;
  }

  /** A run as the experiment sees it: configuration, request identities, and its seeded keys. */
  record RunContext(
      long runId, RunConfig config, List<Run.Request> requests, Map<String, Object> keys) {
    public long key(String name) {
      return ((Number) keys.get(name)).longValue();
    }

    /** A seeded row's UUID (products, orders: the commerce model's ids). */
    public java.util.UUID uuid(String name) {
      return (java.util.UUID) keys.get(name);
    }
  }
}
