package io.zeroshift.racelab.application.port;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/** The lab's PostgreSQL: sessions for requests, autocommit access for seeding and observing. */
public interface LabDatabase {
  /** A new connection named {@code applicationName} in {@code pg_stat_activity}. */
  Session openSession(String applicationName);

  /** Autocommit statements on a connection of the engine's own (seeding, committed state). */
  List<Map<String, Object>> query(String sql, Object... params);

  default Map<String, Object> one(String sql, Object... params) {
    var rows = query(sql, params);
    return rows.isEmpty() ? Map.of() : rows.getFirst();
  }

  int update(String sql, Object... params);

  /** INSERT … RETURNING id. */
  long insert(String sql, Object... params);

  /** For each of {@code pids} that is waiting: the backends holding what it waits for. */
  Map<Integer, Waiting> waiting(Collection<Integer> pids);

  /** A server setting ({@code SHOW deadlock_timeout}). */
  String setting(String name);

  /** Cancels the statement a backend is running (a run that exceeded its time limit). */
  void cancel(int pid);

  /** Deletes every row of the lab's scenario tables; returns rows deleted per table. */
  Map<String, Integer> resetScenarios();

  /** A backend blocked by others, as {@code pg_stat_activity} and {@code pg_blocking_pids} see it. */
  record Waiting(List<Integer> blockedBy, String waitEventType, String waitEvent) {}
}
