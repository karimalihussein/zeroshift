package io.zeroshift.racelab.application;

import io.zeroshift.racelab.domain.Isolation;
import io.zeroshift.racelab.domain.Mode;
import io.zeroshift.racelab.domain.Run;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * One request as an experiment scripts it. Every call is a real step of a real transaction and is
 * recorded as an event with what PostgreSQL returned; nothing here is simulated.
 */
public interface Participant {
  String lane();

  int index();

  /** What this request does when requests differ; null otherwise. */
  String role();

  Run.Request identity();

  Mode mode();

  Isolation isolation();

  int attempt();

  long key(String name);

  /** The run's configured starting value (stock, balance…). */
  int initialValue();

  void begin();

  /** A plain SELECT of one row. */
  Row read(Read read);

  /** A SELECT … FOR UPDATE (or FOR SHARE): records the lock request, any wait, and the lock. */
  Row lockRead(Read read, String lock);

  /**
   * An INSERT or UPDATE. An UPDATE row-locks what it matches until the transaction ends, so a
   * write that could wait records its lock like {@link #lockRead}.
   */
  int write(Write write);

  /** The application's check on what it read. */
  void decide(boolean ok, String rule, String because);

  /** The configured delay between read and write: the application doing its work. */
  void think();

  /**
   * CONTROLLED interleaving: waits until every other request has reached {@code point}, finished,
   * or is blocked inside PostgreSQL. A no-op for NATURAL interleaving.
   */
  void sync(String point);

  /**
   * CONTROLLED interleaving: runs {@code section} only after every earlier request (A before B…)
   * has finished it, finished altogether, or is blocked inside PostgreSQL.
   */
  <T> T inTurn(String point, Supplier<T> section);

  default void inTurn(String point, Runnable section) {
    inTurn(
        point,
        () -> {
          section.run();
          return null;
        });
  }

  /** Commits; the request succeeded with {@code result} (reserved, charged…). */
  void commit(String result);

  /** Rolls back; the request declined with {@code reason} and changed nothing. */
  void reject(String reason);

  /**
   * An optimistic write matched no row: reads the current version, records the conflict, rolls
   * back and throws {@link Aborted}.
   */
  Aborted versionConflict(String target, long expected, Read current);

  /** A read's single row (or none). */
  record Row(Map<String, Object> values) {
    public boolean present() {
      return !values.isEmpty();
    }

    public long number(String column) {
      return ((Number) values.get(column)).longValue();
    }

    public int integer(String column) {
      return (int) number(column);
    }

    public String text(String column) {
      var v = values.get(column);
      return v == null ? null : v.toString();
    }

    public boolean flag(String column) {
      return Boolean.TRUE.equals(values.get(column));
    }
  }

  /**
   * A single-row SELECT. {@code value} names the column shown as the value read ("stock"), {@code
   * version} the row-version column if any.
   */
  record Read(String target, String sql, List<Object> params, String value, String version) {
    public static Read of(String target, String sql, String value, Object... params) {
      return new Read(target, sql, List.of(params), value, null);
    }

    public Read versioned(String column) {
      return new Read(target, sql, params, value, column);
    }
  }

  /**
   * An INSERT or UPDATE. {@code shows} is the value as the timeline prints it ("stock=0"); {@code
   * lock} describes the row lock an UPDATE takes (null for an INSERT of a new row).
   */
  record Write(
      String target, String sql, List<Object> params, String shows, Long version, String lock) {
    public static Write update(String target, String sql, String shows, Object... params) {
      return new Write(target, sql, List.of(params), shows, null, "row lock on " + target);
    }

    public static Write insert(String target, String sql, String shows, Object... params) {
      return new Write(target, sql, List.of(params), shows, null, null);
    }

    public Write version(long written) {
      return new Write(target, sql, params, shows, written, lock);
    }
  }

  /** The transaction was aborted (and already rolled back); the engine may retry the request. */
  final class Aborted extends RuntimeException {
    public enum Why {
      VERSION_CONFLICT,
      SERIALIZATION_FAILURE,
      DEADLOCK,
      LOCK_NOT_AVAILABLE
    }

    private final Why why;

    public Aborted(Why why, String message) {
      super(message, null, false, false);
      this.why = why;
    }

    public Why why() {
      return why;
    }
  }
}
