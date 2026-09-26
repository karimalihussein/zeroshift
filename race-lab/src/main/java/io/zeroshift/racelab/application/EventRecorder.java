package io.zeroshift.racelab.application;

import io.zeroshift.racelab.application.port.RunRepository;
import io.zeroshift.racelab.domain.RaceEvent;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Numbers, timestamps and keeps a run's events. Time is one monotonic clock for the whole run
 * (microseconds since the run began), so events of different threads order correctly. Events are
 * published to live listeners at once and written to PostgreSQL by a background thread, so
 * persisting them never adds latency to the transactions being observed.
 */
final class EventRecorder implements AutoCloseable {
  private final long runId;
  private final long startNanos = System.nanoTime();
  private final Instant startWall = Instant.now();
  private final List<RaceEvent> events = new ArrayList<>();
  private final LinkedBlockingQueue<RaceEvent> unsaved = new LinkedBlockingQueue<>();
  private final RunRepository repository;
  private final RunStreams streams;
  private final Thread writer;
  private volatile boolean open = true;
  private int seq;

  EventRecorder(long runId, RunRepository repository, RunStreams streams) {
    this.runId = runId;
    this.repository = repository;
    this.streams = streams;
    writer = Thread.ofPlatform().name("race-" + runId + "-events").daemon().start(this::write);
  }

  long micros() {
    return (System.nanoTime() - startNanos) / 1000;
  }

  Instant wall(long micros) {
    return startWall.plus(micros, ChronoUnit.MICROS);
  }

  RaceEvent record(RaceEvent.Builder builder) {
    RaceEvent event;
    synchronized (this) {
      long now = micros();
      event = builder.build(runId, ++seq, wall(now), now);
      events.add(event);
    }
    unsaved.add(event);
    streams.publish(event);
    return event;
  }

  synchronized List<RaceEvent> events() {
    return List.copyOf(events);
  }

  private void write() {
    var batch = new ArrayList<RaceEvent>();
    while (open || !unsaved.isEmpty()) {
      try {
        var first = unsaved.poll(50, TimeUnit.MILLISECONDS);
        if (first == null) continue;
        batch.add(first);
        unsaved.drainTo(batch);
        repository.append(batch);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (RuntimeException e) {
        // Keep recording in memory; the final flush retries whatever is left.
        unsaved.addAll(batch);
        sleepQuietly();
      }
      batch.clear();
    }
  }

  /** Waits until every event is stored. */
  @Override
  public void close() {
    open = false;
    try {
      writer.join(TimeUnit.SECONDS.toMillis(10));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    var rest = new ArrayList<RaceEvent>();
    unsaved.drainTo(rest);
    if (!rest.isEmpty()) repository.append(rest);
  }

  private static void sleepQuietly() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
