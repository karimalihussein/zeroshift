package io.zeroshift.racelab.application.port;

import io.zeroshift.racelab.domain.RaceEvent;
import io.zeroshift.racelab.domain.Run;
import io.zeroshift.racelab.domain.RunConfig;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Runs and their events, kept so a finished run can be inspected and compared later. */
public interface RunRepository {
  Run create(RunConfig config, List<Run.Request> requests);

  void save(Run run);

  Optional<Run> find(long id);

  /** Newest first; {@code experiment} null for all. Fetches up to {@code limit} runs. */
  List<Run> recent(String experiment, int limit);

  void append(List<RaceEvent> events);

  /** Events with {@code seq > afterSeq}, in order, up to {@code limit}. */
  List<RaceEvent> events(long runId, int afterSeq, int limit);

  /** Deletes every run and event; returns rows deleted per table. */
  Map<String, Integer> deleteAll();
}
