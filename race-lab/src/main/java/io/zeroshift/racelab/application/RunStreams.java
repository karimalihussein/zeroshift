package io.zeroshift.racelab.application;

import io.zeroshift.racelab.domain.RaceEvent;
import io.zeroshift.racelab.domain.Run;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Live listeners of running runs (the timeline's server-sent events). Delivery happens on one
 * dispatcher thread, in recording order: a slow or replaying listener must never hold up the
 * transaction and lock-monitor threads whose timing the run measures.
 */
public class RunStreams implements AutoCloseable {
  public interface Listener {
    void event(RaceEvent event);

    void finished(Run run);
  }

  private final Map<Long, List<Listener>> listeners = new ConcurrentHashMap<>();
  private final java.util.concurrent.ExecutorService dispatcher =
      java.util.concurrent.Executors.newSingleThreadExecutor(
          Thread.ofPlatform().name("race-lab-streams").daemon().factory());
  private final Map<Long, java.util.function.Supplier<List<RaceEvent>>> live = new ConcurrentHashMap<>();

  /**
   * Every event a running run has recorded so far, from memory: events are persisted in the
   * background, so a late subscriber must not rely on the table alone. Empty once the run ended.
   */
  public java.util.Optional<List<RaceEvent>> recorded(long runId) {
    var source = live.get(runId);
    return source == null ? java.util.Optional.empty() : java.util.Optional.of(source.get());
  }

  void running(long runId, java.util.function.Supplier<List<RaceEvent>> events) {
    live.put(runId, events);
  }

  public Runnable subscribe(long runId, Listener listener) {
    listeners.computeIfAbsent(runId, id -> new CopyOnWriteArrayList<>()).add(listener);
    return () -> {
      var list = listeners.get(runId);
      if (list != null) list.remove(listener);
    };
  }

  void publish(RaceEvent event) {
    dispatcher.execute(() -> deliver(event));
  }

  private void deliver(RaceEvent event) {
    for (var l : listeners.getOrDefault(event.runId(), List.of())) {
      try {
        l.event(event);
      } catch (RuntimeException e) {
        // A listener that cannot keep up (a closed browser tab) never slows the run.
      }
    }
  }

  void finished(Run run) {
    live.remove(run.id());
    dispatcher.execute(() -> deliverFinished(run));
  }

  private void deliverFinished(Run run) {
    var list = listeners.remove(run.id());
    if (list == null) return;
    for (var l : list) {
      try {
        l.finished(run);
      } catch (RuntimeException e) {
        // as above
      }
    }
  }

  @Override
  public void close() {
    dispatcher.shutdownNow();
  }
}
