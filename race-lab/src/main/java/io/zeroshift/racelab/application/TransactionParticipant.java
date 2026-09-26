package io.zeroshift.racelab.application;

import io.zeroshift.racelab.application.Experiment.RunContext;
import io.zeroshift.racelab.application.port.LabDatabase;
import io.zeroshift.racelab.application.port.Session;
import io.zeroshift.racelab.application.port.Session.DbFailure;
import io.zeroshift.racelab.application.port.Tracing;
import io.zeroshift.racelab.domain.EventType;
import io.zeroshift.racelab.domain.Isolation;
import io.zeroshift.racelab.domain.Mode;
import io.zeroshift.racelab.domain.RaceEvent;
import io.zeroshift.racelab.domain.Run;
import io.zeroshift.racelab.domain.RunResult.Outcome;
import io.zeroshift.racelab.domain.RunResult.RequestResult;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A request as a real transaction on its own connection, recording each step as it happens.
 *
 * <p>Lock waits are PostgreSQL's: while a statement is in flight the {@link LockMonitor} asks
 * {@code pg_blocking_pids()} whether this backend waits and for whom. When the statement returns,
 * the wait is closed with its measured length. Aborts are PostgreSQL's too: SQLSTATE 40001 and
 * 40P01 become events and an {@link Aborted}, after the transaction is rolled back.
 */
final class TransactionParticipant implements Participant, LockMonitor.Watched {
  private static final Pattern PROCESS = Pattern.compile("process (\\d+)");

  private final RunContext run;
  private final int index;
  private final String lane;
  private final Run.Request identity;
  private final Isolation isolation;
  private final EventRecorder rec;
  private final Choreography choreography;
  private final Tracing tracing;
  private final LabDatabase db;
  private final LockMonitor monitor;
  private final Consumer<String> committedOrRolledBack;
  private final String instance;

  /** Every request's backend pid, shared by the run's requests (deadlock details name them). */
  private final Map<Integer, String> backends;

  private Session session;
  private int pid;
  private Long txId;
  private int attempt;
  private boolean inTransaction;
  private Tracing.TraceSpan span;
  private final List<String> held = new ArrayList<>();

  private volatile long inFlight;
  private volatile Tracing.TraceSpan statementSpan;
  private long tokens;
  private String currentTarget;
  private String currentLock;

  // Guarded by this: the wait of the statement in flight, as the monitor reported it.
  private long blockedToken;
  private long blockedSince;
  private List<String> blockers = List.of();
  private long lockWaitMicros;
  private long syncWaitMicros;

  private Outcome outcome;
  private String detail;
  private long firstBegin = -1;
  private long end;

  TransactionParticipant(
      RunContext run,
      int index,
      Isolation isolation,
      EventRecorder rec,
      Choreography choreography,
      Tracing tracing,
      LabDatabase db,
      LockMonitor monitor,
      Consumer<String> committedOrRolledBack,
      String instance,
      Map<Integer, String> backends) {
    this.run = run;
    this.index = index;
    this.identity = run.requests().get(index);
    this.lane = identity.lane();
    this.isolation = isolation;
    this.rec = rec;
    this.choreography = choreography;
    this.tracing = tracing;
    this.db = db;
    this.monitor = monitor;
    this.committedOrRolledBack = committedOrRolledBack;
    this.instance = instance;
    this.backends = backends;
  }

  // ---- identity --------------------------------------------------------------------------------

  @Override
  public String lane() {
    return lane;
  }

  @Override
  public int index() {
    return index;
  }

  @Override
  public String role() {
    return identity.role();
  }

  @Override
  public Run.Request identity() {
    return identity;
  }

  @Override
  public Mode mode() {
    return run.config().mode();
  }

  @Override
  public Isolation isolation() {
    return isolation;
  }

  @Override
  public int attempt() {
    return attempt;
  }

  @Override
  public long key(String name) {
    return run.key(name);
  }

  @Override
  public java.util.UUID uuid(String name) {
    return run.uuid(name);
  }

  @Override
  public int initialValue() {
    return run.config().initialValue();
  }

  @Override
  public int pid() {
    return pid;
  }

  @Override
  public long inFlight() {
    return inFlight;
  }

  // ---- transaction steps -----------------------------------------------------------------------

  void startAttempt(int number) {
    attempt = number;
    outcome = null;
    detail = null;
    held.clear();
  }

  @Override
  public void begin() {
    if (session == null) {
      session = db.openSession("race-lab run " + run.runId() + " " + lane);
      pid = session.pid();
      backends.put(pid, lane);
      monitor.watch(this, lane);
    }
    long start = rec.micros();
    if (firstBegin < 0) firstBegin = start;
    span =
        tracing.start(
            "race-lab tx " + lane,
            Map.of(
                "race.run", String.valueOf(run.runId()),
                "race.lane", lane,
                "race.attempt", String.valueOf(attempt),
                "race.mode", mode().name(),
                "race.request_id", identity.requestId(),
                "db.isolation", isolation.sql(),
                "db.pg_backend_pid", String.valueOf(pid)));
    session.begin(isolation);
    txId = session.txId();
    span.attribute("db.txid", String.valueOf(txId));
    inTransaction = true;
    var data = new LinkedHashMap<String, Object>();
    data.put("instance", instance);
    data.put("applicationName", "race-lab run " + run.runId() + " " + lane);
    if (isolation != Isolation.READ_COMMITTED)
      data.put("snapshot", "taken at BEGIN: every read sees the database as of this moment");
    rec.record(
        event(EventType.TRANSACTION_STARTED)
            .startedAt(rec.wall(start), start)
            .durationMicros(rec.micros() - start)
            .sql("BEGIN ISOLATION LEVEL " + isolation.sql() + "; SELECT pg_current_xact_id()")
            .message("transaction " + txId + " on backend " + pid)
            .data(data));
  }

  @Override
  public Row read(Read read) {
    var t =
        statement(
            read.target(),
            null,
            SqlText.render(read.sql(), read.params()),
            () -> session.query(read.sql(), read.params()));
    var row = first(t.value());
    rec.record(readEvent(read, row, t).message(row.present() ? null : "no row"));
    return row;
  }

  @Override
  public Row lockRead(Read read, String lock) {
    long requested = rec.micros();
    rec.record(
        event(EventType.LOCK_REQUESTED)
            .target(read.target())
            .lock(lock)
            .sql(SqlText.render(read.sql(), read.params())));
    var t =
        statement(
            read.target(),
            lock,
            SqlText.render(read.sql(), read.params()),
            () -> session.query(read.sql(), read.params()));
    var row = first(t.value());
    held.add(lock);
    rec.record(acquired(lock, read.target(), requested, t.waited()));
    rec.record(readEvent(read, row, t));
    return row;
  }

  @Override
  public int write(Write write) {
    long requested = rec.micros();
    if (write.lock() != null)
      rec.record(
          event(EventType.LOCK_REQUESTED)
              .target(write.target())
              .lock(write.lock())
              .sql(SqlText.render(write.sql(), write.params())));
    var t =
        statement(
            write.target(),
            write.lock(),
            SqlText.render(write.sql(), write.params()),
            () -> session.update(write.sql(), write.params()));
    int rows = t.value();
    if (write.lock() != null && rows > 0) {
      held.add(write.lock());
      rec.record(acquired(write.lock(), write.target(), requested, t.waited()));
    }
    rec.record(
        event(EventType.WRITE_PERFORMED)
            .startedAt(rec.wall(t.start()), t.start())
            .durationMicros(t.end() - t.start())
            .target(write.target())
            .sql(SqlText.render(write.sql(), write.params()))
            .rows(rows)
            .valueWritten(rows > 0 ? write.shows() : null)
            .versionWritten(rows > 0 ? write.version() : null)
            .waitMicros(t.waited() > 0 ? t.waited() : null)
            .message(
                rows > 0
                    ? null
                    : t.waited() > 0
                        ? "waited " + ms(t.waited()) + " for the row lock, then matched no row"
                        : "matched no row"));
    return rows;
  }

  @Override
  public void decide(boolean ok, String rule, String because) {
    rec.record(event(EventType.DECISION_MADE).ok(ok).target(rule).message(because));
  }

  @Override
  public void think() {
    int delay = run.config().delayMs();
    if (delay <= 0) return;
    long start = rec.micros();
    try {
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    rec.record(
        event(EventType.DELAY)
            .startedAt(rec.wall(start), start)
            .durationMicros(rec.micros() - start)
            .message(
                "application work between read and write (configured delay " + delay + " ms)"));
  }

  @Override
  public void sync(String point) {
    if (!choreography.controlled()) return;
    held(choreography.awaitAll(index, point), point, "until every request reached it");
  }

  @Override
  public <T> T inTurn(String point, Supplier<T> section) {
    if (!choreography.controlled()) return section.get();
    held(choreography.awaitTurn(index, point), point, "for the requests before it");
    try {
      return section.get();
    } finally {
      choreography.passed(index, point);
    }
  }

  private void held(Choreography.Wait wait, String point, String why) {
    syncWaitMicros += wait.micros();
    if (wait.micros() < 500 && !wait.gaveUp()) return;
    long now = rec.micros();
    rec.record(
        event(EventType.SYNC_POINT)
            .startedAt(rec.wall(now - wait.micros()), now - wait.micros())
            .durationMicros(wait.micros())
            .target(point)
            .message(
                "lab choreography held this request at '"
                    + point
                    + "' "
                    + why
                    + (wait.gaveUp() ? " (gave up after 15 s)" : "")));
  }

  @Override
  public void commit(String result) {
    var t =
        statement(
            null,
            null,
            "COMMIT",
            () -> {
              session.commit();
              return null;
            });
    inTransaction = false;
    rec.record(
        event(EventType.TRANSACTION_COMMITTED)
            .startedAt(rec.wall(t.start()), t.start())
            .durationMicros(t.end() - t.start())
            .sql("COMMIT")
            .message(result));
    released("commit");
    outcome = Outcome.SUCCEEDED;
    detail = result;
    end = rec.micros();
    closeSpan(null);
    committedOrRolledBack.accept(lane);
  }

  @Override
  public void reject(String reason) {
    rolledBack(reason);
    outcome = Outcome.REJECTED;
    detail = reason;
  }

  @Override
  public Aborted versionConflict(String target, long expected, Read current) {
    var t =
        statement(
            current.target(),
            null,
            SqlText.render(current.sql(), current.params()),
            () -> session.query(current.sql(), current.params()));
    var row = first(t.value());
    long actual = row.number(current.version() != null ? current.version() : current.value());
    rec.record(
        readEvent(current, row, t)
            .message("re-read after the write matched no row")
            .data(Map.of("purpose", "diagnose")));
    var message =
        "expected version="
            + expected
            + ", actual version="
            + actual
            + ": another transaction committed first";
    rec.record(
        event(EventType.VERSION_CONFLICT)
            .target(target)
            .expectedVersion(expected)
            .actualVersion(actual)
            .message(message));
    rolledBack("stale version " + expected + " rejected");
    return new Aborted(Aborted.Why.VERSION_CONFLICT, message);
  }

  // ---- engine hooks ----------------------------------------------------------------------------

  void aborted(Aborted a) {
    outcome = Outcome.ABORTED;
    detail = a.getMessage();
    end = rec.micros();
  }

  void failed(Throwable t) {
    if (inTransaction) {
      try {
        rolledBack("error: " + t.getMessage());
      } catch (RuntimeException ignored) {
        // already reported by the original failure
      }
    }
    outcome = Outcome.FAILED;
    detail = String.valueOf(t.getMessage());
    end = rec.micros();
  }

  boolean finished() {
    return outcome != null;
  }

  RequestResult result() {
    return new RequestResult(
        lane,
        identity.requestId(),
        identity.orderId(),
        identity.customerId(),
        identity.role(),
        outcome == null ? Outcome.FAILED : outcome,
        attempt,
        firstBegin < 0 ? 0 : Math.max(0, (end > 0 ? end : rec.micros()) - firstBegin),
        lockWaitMicros,
        syncWaitMicros,
        detail);
  }

  void close() {
    if (session != null) session.close();
  }

  RaceEvent.Builder event(EventType type) {
    return RaceEvent.of(type, lane)
        .attempt(attempt)
        .requestId(identity.requestId())
        .txId(txId)
        .pid(pid == 0 ? null : pid)
        .thread(Thread.currentThread().getName())
        .isolation(isolation.sql())
        .trace(span == null ? null : span.traceId(), span == null ? null : span.spanId());
  }

  // ---- lock waits (monitor thread) -------------------------------------------------------------

  @Override
  public synchronized void waiting(
      long token, LabDatabase.Waiting waiting, Map<Integer, String> lanes) {
    if (inFlight != token) return; // that statement already returned
    boolean isWaiting = waiting != null && !waiting.blockedBy().isEmpty();
    if (isWaiting) {
      var by = waiting.blockedBy().stream().map(p -> lanes.getOrDefault(p, "pid " + p)).toList();
      if (blockedToken == token && by.equals(blockers)) return;
      boolean first = blockedToken != token;
      if (first) blockedSince = rec.micros();
      blockedToken = token;
      blockers = by;
      rec.record(
          event(EventType.TRANSACTION_BLOCKED)
              .target(currentTarget)
              .lock(currentLock)
              .blockedBy(by)
              .message(
                  (first ? "waiting for " : "now waiting for ")
                      + (currentLock != null ? "the " + currentLock : "a lock")
                      + " held by "
                      + String.join(", ", by))
              .data(
                  Map.of(
                      "waitEventType", Objects.toString(waiting.waitEventType(), ""),
                      "waitEvent", Objects.toString(waiting.waitEvent(), ""))));
      var s = statementSpan;
      if (s != null)
        s.event(
            "lock wait",
            Map.of(
                "race.blocked_by",
                String.join(",", by),
                "db.lock",
                Objects.toString(currentLock, "")));
      choreography.blocked(index, true);
    } else if (blockedToken == token) {
      unblocked(token, "lock granted");
    }
  }

  private synchronized long unblocked(long token, String why) {
    if (blockedToken != token) return 0;
    long waited = rec.micros() - blockedSince;
    blockedToken = 0;
    lockWaitMicros += waited;
    rec.record(
        event(EventType.TRANSACTION_UNBLOCKED)
            .startedAt(rec.wall(blockedSince), blockedSince)
            .durationMicros(waited)
            .waitMicros(waited)
            .target(currentTarget)
            .lock(currentLock)
            .blockedBy(blockers)
            .message(why + " after " + ms(waited)));
    var s = statementSpan;
    if (s != null) s.event("lock granted", Map.of("race.wait_ms", String.valueOf(waited / 1000)));
    choreography.blocked(index, false);
    return waited;
  }

  // ---- statements ------------------------------------------------------------------------------

  private record Timed<T>(T value, long start, long end, long waited) {}

  private <T> Timed<T> statement(String target, String lock, String sql, Supplier<T> call) {
    long token;
    synchronized (this) {
      token = ++tokens;
      currentTarget = target;
      currentLock = lock;
    }
    long start = rec.micros();
    long waitedBefore = lockWaitMicros;
    var span =
        tracing.start(
            sql == null ? "postgresql" : sql.split(" ", 2)[0] + " " + Objects.toString(target, ""),
            Map.of(
                "db.system",
                "postgresql",
                "db.statement",
                Objects.toString(sql, ""),
                "race.lane",
                lane,
                "db.pg_backend_pid",
                String.valueOf(pid)));
    statementSpan = span;
    inFlight = token;
    T value;
    try {
      value = call.get();
    } catch (DbFailure f) {
      inFlight = 0;
      unblocked(token, "statement failed");
      span.attribute("db.sqlstate", Objects.toString(f.sqlState(), ""));
      span.error(f.getMessage());
      statementSpan = null;
      span.close();
      throw aborted(f);
    } finally {
      inFlight = 0;
    }
    unblocked(token, "lock granted");
    statementSpan = null;
    if (value instanceof Integer rows) span.attribute("db.rows_affected", rows.toString());
    span.close();
    long end = rec.micros();
    return new Timed<>(value, start, end, lockWaitMicros - waitedBefore);
  }

  /** Records PostgreSQL's abort, rolls back, and returns what the request should throw. */
  private RuntimeException aborted(DbFailure f) {
    var why =
        switch (f.kind()) {
          case SERIALIZATION -> Aborted.Why.SERIALIZATION_FAILURE;
          case DEADLOCK -> Aborted.Why.DEADLOCK;
          case LOCK_NOT_AVAILABLE -> Aborted.Why.LOCK_NOT_AVAILABLE;
          default -> null;
        };
    if (why == null) return f;
    if (why != Aborted.Why.LOCK_NOT_AVAILABLE) {
      var data = new LinkedHashMap<String, Object>();
      if (f.detail() != null) data.put("postgresDetail", namedProcesses(f.detail()));
      boolean deadlock = why == Aborted.Why.DEADLOCK;
      rec.record(
          event(deadlock ? EventType.DEADLOCK_DETECTED : EventType.SERIALIZATION_FAILURE)
              .target(currentTarget)
              .sqlState(f.sqlState())
              .message(f.getMessage())
              .blockedBy(deadlock ? deadlockPartners(f.detail()) : null)
              .data(data));
    }
    rolledBack(
        switch (why) {
          case SERIALIZATION_FAILURE -> "aborted by PostgreSQL: serialization failure";
          case DEADLOCK -> "aborted by PostgreSQL: deadlock victim";
          default -> "aborted: " + f.getMessage();
        });
    return new Aborted(why, f.getMessage());
  }

  private void rolledBack(String reason) {
    long start = rec.micros();
    try {
      session.rollback();
    } finally {
      inTransaction = false;
    }
    rec.record(
        event(EventType.TRANSACTION_ROLLED_BACK)
            .startedAt(rec.wall(start), start)
            .durationMicros(rec.micros() - start)
            .sql("ROLLBACK")
            .message(reason));
    released("rollback");
    end = rec.micros();
    closeSpan(reason);
    committedOrRolledBack.accept(lane);
  }

  private void released(String how) {
    for (var lock : held)
      rec.record(event(EventType.LOCK_RELEASED).lock(lock).message("released at " + how));
    held.clear();
  }

  private void closeSpan(String error) {
    if (span == null) return;
    if (error != null) span.error(error);
    span.close();
    span = null;
  }

  private RaceEvent.Builder acquired(String lock, String target, long requested, long waited) {
    return event(EventType.LOCK_ACQUIRED)
        .target(target)
        .lock(lock)
        .waitMicros(waited)
        .durationMicros(rec.micros() - requested)
        .message(waited > 0 ? "granted after waiting " + ms(waited) : "granted immediately");
  }

  private RaceEvent.Builder readEvent(Read read, Row row, Timed<?> t) {
    Object value = row.values().get(read.value());
    return event(EventType.READ_PERFORMED)
        .startedAt(rec.wall(t.start()), t.start())
        .durationMicros(t.end() - t.start())
        .target(read.target())
        .sql(SqlText.render(read.sql(), read.params()))
        .valueRead(row.present() ? read.value() + "=" + value : null)
        .versionRead(read.version() != null && row.present() ? row.number(read.version()) : null);
  }

  private static Row first(List<Map<String, Object>> rows) {
    return new Row(rows.isEmpty() ? Map.of() : rows.getFirst());
  }

  private String namedProcesses(String detail) {
    Matcher m = PROCESS.matcher(detail);
    var out = new StringBuilder();
    while (m.find()) {
      String name = laneOf(Integer.parseInt(m.group(1)));
      m.appendReplacement(out, "process " + m.group(1) + (name == null ? "" : " (" + name + ")"));
    }
    m.appendTail(out);
    return out.toString();
  }

  private List<String> deadlockPartners(String detail) {
    if (detail == null) return null;
    var found = new ArrayList<String>();
    Matcher m = PROCESS.matcher(detail);
    while (m.find()) {
      var name = laneOf(Integer.parseInt(m.group(1)));
      if (name != null && !name.equals(lane) && !found.contains(name)) found.add(name);
    }
    return found;
  }

  private String laneOf(int backend) {
    return backends.get(backend);
  }

  static String ms(long micros) {
    return micros >= 10_000
        ? Math.round(micros / 1000.0) + " ms"
        : String.format(java.util.Locale.ROOT, "%.1f ms", micros / 1000.0);
  }
}
