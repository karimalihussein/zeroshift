package io.zeroshift.racelab.application;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Makes a race reproducible without faking it (CONTROLLED interleaving). Requests take turns at a
 * named section (A reads, then B reads) and meet at barriers (nobody writes until everyone has
 * read). A request counts as having arrived when it is <em>blocked inside PostgreSQL</em>, as the
 * lock monitor reports: the choreography only decides when each request sends its next statement
 * and never waits for a request the database itself is holding.
 */
final class Choreography {
  static final long GIVE_UP_NANOS = TimeUnit.SECONDS.toNanos(15);

  private final boolean controlled;
  private final Lane[] lanes;

  private static final class Lane {
    final Set<String> reached = new HashSet<>();
    final Set<String> passed = new HashSet<>();
    boolean blocked;
    boolean done;
  }

  /** How long a request was held, and whether the choreography gave up on someone. */
  record Wait(long micros, boolean gaveUp) {}

  Choreography(int requests, boolean controlled) {
    this.controlled = controlled;
    lanes = new Lane[requests];
    for (int i = 0; i < requests; i++) lanes[i] = new Lane();
  }

  boolean controlled() {
    return controlled;
  }

  synchronized void blocked(int index, boolean blocked) {
    lanes[index].blocked = blocked;
    notifyAll();
  }

  synchronized void done(int index) {
    lanes[index].done = true;
    notifyAll();
  }

  synchronized void passed(int index, String point) {
    lanes[index].passed.add(point);
    notifyAll();
  }

  /** Barrier: every other request has reached or passed {@code point}, finished, or is blocked. */
  synchronized Wait awaitAll(int index, String point) {
    lanes[index].reached.add(point);
    notifyAll();
    return await(
        () -> {
          for (int j = 0; j < lanes.length; j++) {
            if (j == index) continue;
            var o = lanes[j];
            if (!(o.reached.contains(point) || o.passed.contains(point) || o.done || o.blocked))
              return false;
          }
          return true;
        });
  }

  /** Turn: every earlier request has passed {@code point}, finished, or is blocked. */
  synchronized Wait awaitTurn(int index, String point) {
    return await(
        () -> {
          for (int j = 0; j < index; j++) {
            var o = lanes[j];
            if (!(o.passed.contains(point) || o.done || o.blocked)) return false;
          }
          return true;
        });
  }

  private Wait await(java.util.function.BooleanSupplier ready) {
    long start = System.nanoTime();
    long deadline = start + GIVE_UP_NANOS;
    while (!ready.getAsBoolean()) {
      long left = deadline - System.nanoTime();
      if (left <= 0) return new Wait((System.nanoTime() - start) / 1000, true);
      try {
        TimeUnit.NANOSECONDS.timedWait(this, Math.min(left, TimeUnit.MILLISECONDS.toNanos(20)));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return new Wait((System.nanoTime() - start) / 1000, true);
      }
    }
    return new Wait((System.nanoTime() - start) / 1000, false);
  }
}
