package io.zeroshift.racelab.application;

import static io.zeroshift.racelab.application.TransactionParticipant.ms;

import io.zeroshift.racelab.domain.EventType;
import io.zeroshift.racelab.domain.RaceEvent;
import io.zeroshift.racelab.domain.RunResult.Explanation;
import io.zeroshift.racelab.domain.RunResult.Highlight;
import io.zeroshift.racelab.domain.RunResult.InvariantResult;
import io.zeroshift.racelab.domain.RunResult.Outcome;
import io.zeroshift.racelab.domain.RunResult.RequestResult;
import io.zeroshift.racelab.domain.RunResult.RunMetrics;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Turns a run's recorded events into what the lab shows beside the timeline: measured metrics, the
 * moments worth pointing at, and the step-by-step "why". Everything is derived from events; nothing
 * is assumed from the mode that was chosen.
 */
final class RunAnalysis {
  private static final int MAX_STEPS = 80;

  private RunAnalysis() {}

  static RunMetrics metrics(List<RaceEvent> events, List<RequestResult> requests) {
    int succeeded = count(requests, Outcome.SUCCEEDED);
    int rejected = count(requests, Outcome.REJECTED);
    int aborted = count(requests, Outcome.ABORTED) + count(requests, Outcome.FAILED);
    int versions = count(events, EventType.VERSION_CONFLICT);
    int serialization = count(events, EventType.SERIALIZATION_FAILURE);
    int deadlocks = count(events, EventType.DEADLOCK_DETECTED);
    var waits =
        events.stream()
            .filter(e -> e.type() == EventType.TRANSACTION_UNBLOCKED)
            .mapToLong(e -> e.waitMicros() == null ? 0 : e.waitMicros())
            .toArray();
    int lockWaits =
        (int)
            events.stream()
                .filter(e -> e.type() == EventType.TRANSACTION_BLOCKED)
                .map(e -> e.lane() + "#" + e.attempt())
                .distinct()
                .count();
    var latencies = requests.stream().mapToLong(RequestResult::latencyMicros).sorted().toArray();
    long first =
        events.stream()
            .filter(e -> e.type() == EventType.TRANSACTION_STARTED)
            .mapToLong(RaceEvent::atMicros)
            .min()
            .orElse(0);
    long last =
        events.stream()
            .filter(e -> e.type() == EventType.REQUEST_COMPLETED)
            .mapToLong(RaceEvent::atMicros)
            .max()
            .orElse(first);
    long duration = Math.max(1, last - first);
    return new RunMetrics(
        requests.size(),
        succeeded,
        rejected,
        aborted,
        versions + serialization + deadlocks,
        count(events, EventType.RETRY_SCHEDULED),
        lockWaits,
        java.util.Arrays.stream(waits).sum(),
        deadlocks,
        serialization,
        versions,
        percentile(latencies, 50),
        percentile(latencies, 95),
        percentile(latencies, 99),
        latencies.length == 0 ? 0 : latencies[latencies.length - 1],
        duration,
        Math.round(succeeded / (duration / 1_000_000.0) * 10) / 10.0,
        requests.stream().mapToLong(RequestResult::syncWaitMicros).sum());
  }

  /** Nearest-rank percentile. */
  static long percentile(long[] sorted, int p) {
    if (sorted.length == 0) return 0;
    int rank = (int) Math.ceil(p / 100.0 * sorted.length);
    return sorted[Math.max(0, Math.min(sorted.length - 1, rank - 1))];
  }

  static List<Highlight> highlights(List<RaceEvent> events, InvariantResult invariant) {
    var out = new ArrayList<Highlight>();
    sameValueReads(events, invariant, out);
    changedUnderReader(events, out);
    for (var e : events) {
      switch (e.type()) {
        case TRANSACTION_BLOCKED -> {
          if (e.message() != null && e.message().startsWith("now")) break;
          var unblocked = next(events, e, EventType.TRANSACTION_UNBLOCKED);
          out.add(
              new Highlight(
                  "BLOCKED",
                  "wait",
                  e.atMicros(),
                  unblocked == null ? null : unblocked.atMicros(),
                  lanes(e.lane(), e.blockedBy()),
                  seqs(e, unblocked),
                  "Transaction "
                      + e.lane()
                      + " is waiting for "
                      + (e.lock() != null ? "the " + e.lock() : "a lock")
                      + " held by "
                      + String.join(", ", e.blockedBy()),
                  unblocked == null
                      ? "PostgreSQL reports the backend waiting (pg_blocking_pids)."
                      : "It waited " + ms(unblocked.waitMicros()) + ", until the lock was released."));
        }
        case VERSION_CONFLICT ->
            out.add(
                new Highlight(
                    "VERSION_REJECTED",
                    "retry",
                    e.atMicros(),
                    null,
                    List.of(e.lane()),
                    List.of(e.seq()),
                    "Optimistic lock rejected "
                        + e.lane()
                        + " because expected version="
                        + e.expectedVersion()
                        + ", actual version="
                        + e.actualVersion(),
                    "Its UPDATE … WHERE version = "
                        + e.expectedVersion()
                        + " matched no row: someone committed version "
                        + e.actualVersion()
                        + " after "
                        + e.lane()
                        + " read."));
        case SERIALIZATION_FAILURE ->
            out.add(
                new Highlight(
                    "SERIALIZATION_FAILURE",
                    "retry",
                    e.atMicros(),
                    null,
                    List.of(e.lane()),
                    List.of(e.seq()),
                    "Serialization failure: transaction " + e.lane() + " must retry",
                    "PostgreSQL (SQLSTATE " + e.sqlState() + "): " + e.message()));
        case DEADLOCK_DETECTED -> {
          var partners = e.blockedBy() == null ? List.<String>of() : e.blockedBy();
          out.add(
              new Highlight(
                  "DEADLOCK",
                  "bad",
                  e.atMicros(),
                  null,
                  lanes(e.lane(), partners),
                  List.of(e.seq()),
                  "Deadlock: "
                      + cycle(e.lane(), partners)
                      + ". PostgreSQL aborted "
                      + e.lane(),
                  String.valueOf(
                      e.data() == null ? e.message() : e.data().getOrDefault("postgresDetail", e.message()))));
        }
        default -> {}
      }
    }
    staleWrites(events, invariant, out);
    var end = events.isEmpty() ? 0 : events.getLast().atMicros();
    out.add(
        new Highlight(
            invariant.holds() ? "INVARIANT_PRESERVED" : "INVARIANT_VIOLATED",
            invariant.holds() ? "good" : "bad",
            end,
            null,
            List.of(),
            List.of(),
            invariant.holds() ? "Invariant preserved" : "Invariant violated: " + invariant.actual(),
            invariant.statement()));
    out.sort(Comparator.comparingLong(Highlight::atMicros));
    return out;
  }

  /** Two or more requests read the same value of the same row before any of them committed. */
  private static void sameValueReads(
      List<RaceEvent> events, InvariantResult invariant, List<Highlight> out) {
    var byValue = new LinkedHashMap<String, List<RaceEvent>>();
    for (var e : events)
      if (isRead(e)) byValue.computeIfAbsent(e.target() + "|" + e.valueRead(), k -> new ArrayList<>()).add(e);
    for (var reads : byValue.values()) {
      var concurrent = new ArrayList<RaceEvent>();
      for (var r : reads) {
        boolean earlierCommitted =
            concurrent.stream()
                .anyMatch(c -> committedBetween(events, c.lane(), c.atMicros(), completed(r)));
        if (!earlierCommitted && concurrent.stream().noneMatch(c -> c.lane().equals(r.lane())))
          concurrent.add(r);
      }
      if (concurrent.size() < 2) continue;
      var who = concurrent.stream().map(RaceEvent::lane).toList();
      var value = concurrent.getFirst().valueRead().replace("=", " = ");
      out.add(
          new Highlight(
              "SAME_VALUE_READ",
              invariant.holds() ? "info" : "bad",
              concurrent.getFirst().atMicros(),
              concurrent.getLast().atMicros(),
              who,
              concurrent.stream().map(RaceEvent::seq).toList(),
              (who.size() == 2 ? "Both transactions" : String.join(", ", who)) + " read " + value,
              "Each read the same committed value before any of them committed a change, so each"
                  + " decides on a state that will not survive the others."));
    }
  }

  /** The same transaction read the same thing twice and got different answers. */
  private static void changedUnderReader(List<RaceEvent> events, List<Highlight> out) {
    var seen = new LinkedHashMap<String, RaceEvent>();
    for (var e : events) {
      if (!isRead(e)) continue;
      var key = e.lane() + "#" + e.attempt() + "|" + e.target();
      var before = seen.put(key, e);
      if (before != null && !Objects.equals(before.valueRead(), e.valueRead()))
        out.add(
            new Highlight(
                "CHANGED_UNDER_READER",
                "bad",
                before.atMicros(),
                e.atMicros(),
                List.of(e.lane()),
                List.of(before.seq(), e.seq()),
                "Transaction "
                    + e.lane()
                    + " read "
                    + e.target()
                    + " twice: "
                    + before.valueRead()
                    + ", then "
                    + e.valueRead(),
                "Another transaction committed in between, and at "
                    + e.isolation()
                    + " each statement sees the latest committed data."));
    }
  }

  /** A request committed a write based on a read that another request's commit had outdated. */
  private static void staleWrites(
      List<RaceEvent> events, InvariantResult invariant, List<Highlight> out) {
    for (var commit : events) {
      if (commit.type() != EventType.TRANSACTION_COMMITTED) continue;
      var lane = commit.lane();
      // The latest write of this attempt to a row the request had read (not its own inserts).
      RaceEvent write = null;
      RaceEvent read = null;
      for (var e : events) {
        if (e.seq() >= commit.seq()) break;
        if (!e.lane().equals(lane) || !Objects.equals(e.attempt(), commit.attempt())) continue;
        if (e.type() != EventType.WRITE_PERFORMED || e.rows() == null || e.rows() == 0) continue;
        var target = e.target();
        var source = last(events, e, r -> r.lane().equals(lane) && isRead(r) && Objects.equals(r.attempt(), commit.attempt()) && target != null && target.equals(r.target()));
        if (source != null) {
          write = e;
          read = source;
        }
      }
      if (write == null) continue;
      final var readEvent = read;
      final var writeEvent = write;
      var other =
          events.stream()
              .filter(
                  e ->
                      e.type() == EventType.TRANSACTION_COMMITTED
                          && !e.lane().equals(lane)
                          && e.atMicros() > completed(readEvent)
                          && e.atMicros() < commit.atMicros()
                          && wrote(events, e, writeEvent.target()))
              .findFirst()
              .orElse(null);
      if (other == null) continue;
      out.add(
          new Highlight(
              "STALE_WRITE",
              invariant.holds() ? "warn" : "bad",
              write.atMicros(),
              commit.atMicros(),
              List.of(lane, other.lane()),
              List.of(read.seq(), other.seq(), write.seq(), commit.seq()),
              lane
                  + " wrote "
                  + write.valueWritten()
                  + " from its read of "
                  + read.valueRead()
                  + ", after "
                  + other.lane()
                  + " had committed",
              "The write overwrote "
                  + other.lane()
                  + "'s committed change without seeing it: this interleaving is the bug."));
    }
  }

  static Explanation explain(
      List<RaceEvent> events,
      InvariantResult invariant,
      String conclusion,
      String mechanism,
      Explanation.Fix fix) {
    var steps = new ArrayList<Explanation.Step>();
    for (var e : events) {
      var step = step(e);
      if (step != null) steps.add(step);
    }
    if (steps.size() > MAX_STEPS) {
      var head = new ArrayList<>(steps.subList(0, MAX_STEPS - 1));
      head.add(
          new Explanation.Step(
              0, "", "… " + (steps.size() - MAX_STEPS + 1) + " more steps on the timeline", "info"));
      steps = head;
    }
    var headline =
        invariant.holds()
            ? "Invariant preserved: " + invariant.actual()
            : "Invariant violated: " + invariant.actual();
    return new Explanation(headline, steps, conclusion, mechanism, fix);
  }

  private static Explanation.Step step(RaceEvent e) {
    String text =
        switch (e.type()) {
          case READ_PERFORMED ->
              e.data() != null && "diagnose".equals(e.data().get("purpose"))
                  ? e.lane() + " re-read " + e.valueRead() + " to see why"
                  : e.lane() + " read " + (e.valueRead() == null ? "no row" : e.valueRead());
          case DECISION_MADE ->
              e.lane()
                  + " decided "
                  + (e.ok() ? "✓ " : "✗ ")
                  + e.target()
                  + (e.message() == null ? "" : " (" + e.message() + ")");
          case TRANSACTION_BLOCKED ->
              e.lane() + " blocked: " + e.message();
          case LOCK_ACQUIRED ->
              e.waitMicros() != null && e.waitMicros() > 0
                  ? e.lane() + " got " + e.lock() + " after " + ms(e.waitMicros())
                  : e.lane() + " locked " + e.target();
          case WRITE_PERFORMED ->
              e.rows() != null && e.rows() > 0
                  ? e.lane() + " wrote " + e.valueWritten()
                  : e.lane() + "'s write " + e.message();
          case VERSION_CONFLICT -> e.lane() + " was rejected: " + e.message();
          case SERIALIZATION_FAILURE -> e.lane() + " was aborted by PostgreSQL: " + e.message();
          case DEADLOCK_DETECTED ->
              e.lane() + " was chosen as deadlock victim: " + e.message();
          case TRANSACTION_COMMITTED -> e.lane() + " committed" + suffix(e.message());
          case TRANSACTION_ROLLED_BACK -> e.lane() + " rolled back" + suffix(e.message());
          case RETRY_SCHEDULED -> e.lane() + " retried: " + e.message();
          default -> null;
        };
    if (text == null) return null;
    var tone =
        switch (e.type()) {
          case TRANSACTION_BLOCKED -> "wait";
          case VERSION_CONFLICT, SERIALIZATION_FAILURE, RETRY_SCHEDULED -> "retry";
          case DEADLOCK_DETECTED, TRANSACTION_ROLLED_BACK -> "bad";
          case TRANSACTION_COMMITTED -> "good";
          case DECISION_MADE -> e.ok() ? "info" : "bad";
          default -> "info";
        };
    return new Explanation.Step(e.seq(), e.lane(), text, tone);
  }

  // ---- helpers ---------------------------------------------------------------------------------

  /**
   * When a statement returned. A read that waited for a lock began before the lock holder committed
   * but saw the database as of its completion, so staleness is judged against this moment.
   */
  private static long completed(RaceEvent e) {
    return e.atMicros() + (e.durationMicros() == null ? 0 : e.durationMicros());
  }

  private static boolean isRead(RaceEvent e) {
    return e.type() == EventType.READ_PERFORMED
        && e.valueRead() != null
        && (e.data() == null || !"diagnose".equals(e.data().get("purpose")));
  }

  private static boolean committedBetween(List<RaceEvent> events, String lane, long from, long to) {
    return events.stream()
        .anyMatch(
            e ->
                e.lane().equals(lane)
                    && e.type() == EventType.TRANSACTION_COMMITTED
                    && e.atMicros() > from
                    && e.atMicros() < to);
  }

  private static boolean wrote(List<RaceEvent> events, RaceEvent commit, String target) {
    return last(
            events,
            commit,
            e ->
                e.lane().equals(commit.lane())
                    && Objects.equals(e.attempt(), commit.attempt())
                    && e.type() == EventType.WRITE_PERFORMED
                    && e.rows() != null
                    && e.rows() > 0
                    && target.equals(e.target()))
        != null;
  }

  private static RaceEvent last(
      List<RaceEvent> events, RaceEvent before, java.util.function.Predicate<RaceEvent> match) {
    RaceEvent found = null;
    for (var e : events) {
      if (e.seq() >= before.seq()) break;
      if (match.test(e)) found = e;
    }
    return found;
  }

  private static RaceEvent next(List<RaceEvent> events, RaceEvent after, EventType type) {
    return events.stream()
        .filter(
            e ->
                e.seq() > after.seq()
                    && e.type() == type
                    && e.lane().equals(after.lane())
                    && Objects.equals(e.attempt(), after.attempt()))
        .findFirst()
        .orElse(null);
  }

  private static String cycle(String victim, List<String> partners) {
    if (partners.isEmpty()) return victim + " waited in a cycle";
    var p = partners.getFirst();
    return victim + " waits for " + p + ", " + p + " waits for " + victim;
  }

  private static List<String> lanes(String lane, List<String> others) {
    var all = new ArrayList<String>();
    all.add(lane);
    if (others != null) others.stream().filter(o -> !all.contains(o)).forEach(all::add);
    return all;
  }

  private static List<Integer> seqs(RaceEvent a, RaceEvent b) {
    return b == null ? List.of(a.seq()) : List.of(a.seq(), b.seq());
  }

  private static String suffix(String message) {
    return message == null || message.isBlank() ? "" : ": " + message;
  }

  private static int count(List<RaceEvent> events, EventType type) {
    return (int) events.stream().filter(e -> e.type() == type).count();
  }

  private static int count(List<RequestResult> requests, Outcome outcome) {
    return (int) requests.stream().filter(r -> r.outcome() == outcome).count();
  }

  /** The condensed interleaving shown in the comparison: "A READ stock=1", "B WAITS for A"… */
  static List<String> sequence(List<RaceEvent> events) {
    return events.stream()
        .map(
            e ->
                switch (e.type()) {
                  case READ_PERFORMED ->
                      isRead(e) ? e.lane() + " READ " + e.valueRead() : null;
                  case LOCK_ACQUIRED ->
                      e.waitMicros() != null && e.waitMicros() > 0
                          ? e.lane() + " WAKES, gets lock"
                          : e.lane() + " LOCKS " + e.target();
                  case TRANSACTION_BLOCKED ->
                      e.message() != null && e.message().startsWith("now")
                          ? null
                          : e.lane() + " WAITS for " + String.join(", ", e.blockedBy());
                  case WRITE_PERFORMED ->
                      e.rows() != null && e.rows() > 0
                          ? e.lane() + " WRITE " + e.valueWritten()
                          : e.lane() + " WRITE matched 0 rows";
                  case VERSION_CONFLICT ->
                      e.lane()
                          + " STALE v"
                          + e.expectedVersion()
                          + " ≠ v"
                          + e.actualVersion();
                  case SERIALIZATION_FAILURE -> e.lane() + " SERIALIZATION FAILURE";
                  case DEADLOCK_DETECTED -> e.lane() + " DEADLOCK VICTIM";
                  case DECISION_MADE -> e.ok() ? null : e.lane() + " DECLINES";
                  case TRANSACTION_COMMITTED -> e.lane() + " COMMIT";
                  case TRANSACTION_ROLLED_BACK -> e.lane() + " ROLLBACK";
                  case RETRY_SCHEDULED -> e.lane() + " RETRIES";
                  default -> null;
                })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }
}
