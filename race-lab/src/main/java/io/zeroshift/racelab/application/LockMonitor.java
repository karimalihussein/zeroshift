package io.zeroshift.racelab.application;

import io.zeroshift.racelab.application.port.LabDatabase;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Watches the run's backends through PostgreSQL's own view of lock waits ({@code
 * pg_stat_activity.wait_event} and {@code pg_blocking_pids()}), every few milliseconds, on the
 * engine's connection. A request is reported blocked only while it has a statement in flight, and
 * only for that statement, so a wait is never attributed to a statement that already returned.
 */
final class LockMonitor implements AutoCloseable {
  private static final long POLL_MILLIS = 4;

  /** A request's backend, as the monitor needs it. */
  interface Watched {
    int pid();

    /** The token of the statement in flight, or 0 when none is. */
    long inFlight();

    /**
     * PostgreSQL reports this backend waiting (or no longer waiting) during statement {@code
     * token}.
     */
    void waiting(long token, LabDatabase.Waiting waiting, Map<Integer, String> lanes);
  }

  private final LabDatabase db;
  private final List<Watched> watched = new CopyOnWriteArrayList<>();
  private final Map<Integer, String> lanes = new ConcurrentHashMap<>();
  private volatile boolean running = true;
  private final Thread thread;

  LockMonitor(LabDatabase db, long runId) {
    this.db = db;
    thread = Thread.ofPlatform().name("race-" + runId + "-locks").daemon().start(this::loop);
  }

  void watch(Watched w, String lane) {
    lanes.put(w.pid(), lane);
    watched.add(w);
  }

  private void loop() {
    while (running) {
      try {
        var active =
            watched.stream()
                .filter(w -> w.inFlight() != 0)
                .collect(Collectors.toMap(w -> w, Watched::inFlight, (a, b) -> a));
        if (!active.isEmpty()) {
          var waiting = db.waiting(active.keySet().stream().map(Watched::pid).toList());
          active.forEach((w, token) -> w.waiting(token, waiting.get(w.pid()), Map.copyOf(lanes)));
        }
        Thread.sleep(POLL_MILLIS);
      } catch (InterruptedException e) {
        return;
      } catch (RuntimeException e) {
        // The monitor observes; it never fails a run. The next poll tries again.
        try {
          Thread.sleep(50);
        } catch (InterruptedException ie) {
          return;
        }
      }
    }
  }

  @Override
  public void close() {
    running = false;
    thread.interrupt();
  }
}
