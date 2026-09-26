package io.zeroshift.racelab.experiments;

import static io.zeroshift.racelab.experiments.Texts.number;

import io.zeroshift.racelab.application.Experiment;
import io.zeroshift.racelab.application.Participant;
import io.zeroshift.racelab.application.Participant.Write;
import io.zeroshift.racelab.application.port.LabDatabase;
import io.zeroshift.racelab.domain.ExperimentInfo;
import io.zeroshift.racelab.domain.ExperimentInfo.ModeInfo;
import io.zeroshift.racelab.domain.Isolation;
import io.zeroshift.racelab.domain.Mode;
import io.zeroshift.racelab.domain.RaceEvent;
import io.zeroshift.racelab.domain.RunConfig;
import io.zeroshift.racelab.domain.RunResult.Explanation;
import io.zeroshift.racelab.domain.RunResult.InvariantResult;
import io.zeroshift.racelab.domain.RunResult.Outcome;
import io.zeroshift.racelab.domain.RunResult.RequestResult;
import java.util.List;
import java.util.Map;

/**
 * 7. Two transfers in opposite directions. Each locks its source account, then wants the other's:
 * a wait-for cycle that only PostgreSQL's deadlock detector can break, by aborting one of them.
 */
public class Deadlock implements Experiment {
  static final int AMOUNT = 10;

  @Override
  public ExperimentInfo info() {
    return new ExperimentInfo(
        "deadlock",
        7,
        "Deadlock",
        "Locks",
        "Alice pays Bob while Bob pays Alice. Each transaction locks one account, then needs the"
            + " other. Who waits for whom?",
        "Transaction A debits Alice (locking her row), then credits Bob. Transaction B debits Bob"
            + " (locking his row), then credits Alice. Each now waits for a lock the other holds:"
            + " a cycle. Neither can ever proceed, so after deadlock_timeout PostgreSQL's detector"
            + " finds the cycle and aborts one transaction (SQLSTATE 40P01) so the other can"
            + " finish.",
        "every transfer commits, and the total balance is conserved",
        "Initial balance (each)",
        List.of(
            new ModeInfo(
                Mode.UNSAFE,
                "Opposite lock order",
                "Each transfer locks its source account first, then the destination. Two"
                    + " transfers in opposite directions lock the same two rows in opposite order.",
                "-- A: Alice → Bob            -- B: Bob → Alice\n"
                    + "UPDATE account … Alice;     UPDATE account … Bob;\n"
                    + "UPDATE account … Bob;       UPDATE account … Alice;\n"
                    + "-- waits for B               -- waits for A → 40P01",
                Isolation.READ_COMMITTED,
                false),
            new ModeInfo(
                Mode.ORDERED_LOCKS,
                "Consistent lock order",
                "Every transfer locks the two accounts in the same order (lowest id first),"
                    + " whatever the direction. The second transfer waits for the first at its"
                    + " first lock, holding nothing; a cycle cannot form.",
                "-- both: lock the lower id first\n"
                    + "UPDATE account … WHERE id = least(:from, :to);\n"
                    + "UPDATE account … WHERE id = greatest(:from, :to);",
                Isolation.READ_COMMITTED,
                true)),
        Mode.UNSAFE,
        Isolation.READ_COMMITTED,
        new ExperimentInfo.Limits(2, 2, 2, 10, 100_000, 100, 100, 0),
        List.of("Alice → Bob", "Bob → Alice"),
        "A locks Alice · B locks Bob · A wants Bob (waits for B) · B wants Alice (waits for A) →"
            + " cycle → PostgreSQL aborts one.");
  }

  @Override
  public boolean retries(Mode mode) {
    return false;
  }

  @Override
  public Map<String, Object> seed(LabDatabase db, RunContext run) {
    long alice = account(db, run, "Alice");
    long bob = account(db, run, "Bob");
    return Map.of("run", run.runId(), "alice", alice, "bob", bob);
  }

  private static long account(LabDatabase db, RunContext run, String name) {
    return db.insert(
        "INSERT INTO account(run_id, name, balance, version) VALUES (?, ?, ?, 1) RETURNING id",
        run.runId(),
        name,
        run.config().initialValue());
  }

  @Override
  public void attempt(Participant p) {
    long alice = p.key("alice");
    long bob = p.key("bob");
    boolean fromAlice = p.index() % 2 == 0;
    long from = fromAlice ? alice : bob;
    long to = fromAlice ? bob : alice;
    long first = p.mode() == Mode.ORDERED_LOCKS ? Math.min(from, to) : from;
    long second = first == from ? to : from;
    p.begin();
    p.inTurn("first lock", () -> p.write(move(p, first, from, alice)));
    p.sync("first lock");
    p.think();
    p.write(move(p, second, from, alice));
    p.commit("moved " + AMOUNT + " from " + name(from, alice) + " to " + name(to, alice));
  }

  private static Write move(Participant p, long account, long from, long alice) {
    boolean debit = account == from;
    var who = name(account, alice);
    return Write.update(
        who + "'s account #" + account,
        "UPDATE account SET balance = balance " + (debit ? "-" : "+") + " ?, version = version + 1"
            + " WHERE id = ?",
        who + (debit ? " −" : " +") + AMOUNT,
        AMOUNT,
        account);
  }

  private static String name(long account, long alice) {
    return account == alice ? "Alice" : "Bob";
  }

  @Override
  public Map<String, Object> observe(LabDatabase db, RunContext run) {
    var rows =
        db.query(
            "SELECT name, balance FROM account WHERE id IN (?, ?) ORDER BY id",
            run.key("alice"),
            run.key("bob"));
    long a = ((Number) rows.get(0).get("balance")).longValue();
    long b = ((Number) rows.get(1).get("balance")).longValue();
    return Map.of("alice", a, "bob", b, "total", a + b);
  }

  @Override
  public String describe(Map<String, Object> state) {
    return "Alice=" + number(state, "alice") + " · Bob=" + number(state, "bob");
  }

  @Override
  public InvariantResult check(
      RunContext run,
      Map<String, Object> initial,
      Map<String, Object> result,
      List<RequestResult> requests,
      List<RaceEvent> events) {
    int committed = Texts.count(requests, Outcome.SUCCEEDED);
    long total = number(result, "total");
    long expectedTotal = number(initial, "total");
    boolean holds = committed == requests.size() && total == expectedTotal;
    var victims =
        requests.stream()
            .filter(r -> r.outcome() == Outcome.ABORTED)
            .map(RequestResult::lane)
            .toList();
    return new InvariantResult(
        "every transfer commits, and the total balance is conserved",
        holds,
        requests.size() + " transfers committed, total " + expectedTotal,
        committed
            + " of "
            + requests.size()
            + " committed"
            + (victims.isEmpty() ? "" : " (" + String.join(", ", victims) + " aborted as deadlock victim)")
            + ", total "
            + total,
        total == expectedTotal
            ? holds
                ? "No cycle formed; both transfers completed."
                : "No money was lost (the victim rolled back entirely), but a transfer failed."
            : "The total changed.");
  }

  @Override
  public String conclusion(RunContext run, InvariantResult invariant, List<RequestResult> requests) {
    if (invariant.holds())
      return run.config().mode() == Mode.ORDERED_LOCKS
          ? "Both transfers asked for the lower account id first. The second transfer waited at"
              + " its first lock while holding nothing, so the first could take both rows, commit"
              + " and let it through."
          : Texts.preserved(run.config(), requests);
    return "Each transfer held the lock the other needed next. PostgreSQL waited deadlock_timeout,"
        + " found the cycle and aborted one transaction to break it. Its whole transfer rolled"
        + " back: consistent, but the request failed and must be retried by the caller.";
  }

  @Override
  public Explanation.Fix fix(RunConfig config, InvariantResult invariant) {
    if (invariant.holds()) return null;
    return Texts.fix(
        Mode.ORDERED_LOCKS,
        Isolation.READ_COMMITTED,
        "Lock rows in one global order (lowest id first). Then a transaction can only wait for one"
            + " that holds nothing it needs.");
  }
}
