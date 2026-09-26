package io.zeroshift.racelab.experiments;

import static io.zeroshift.racelab.experiments.Texts.mode;
import static io.zeroshift.racelab.experiments.Texts.number;
import static io.zeroshift.racelab.experiments.Texts.plural;

import io.zeroshift.racelab.application.Participant;
import io.zeroshift.racelab.application.Participant.Read;
import io.zeroshift.racelab.application.Participant.Row;
import io.zeroshift.racelab.application.Participant.Write;
import io.zeroshift.racelab.application.port.LabDatabase;
import io.zeroshift.racelab.domain.ExperimentInfo;
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

/** 2. Concurrent deposits into one balance: read-modify-write overwrites a neighbour's change. */
public class LostUpdate extends ReadDecideWrite {
  static final int AMOUNT = 10;
  static final String READ = "SELECT balance, version FROM account WHERE id = ?";

  @Override
  public ExperimentInfo info() {
    return new ExperimentInfo(
        "lost-update",
        2,
        "Lost update",
        "Read-modify-write",
        "Two deposits of 10 land on the same balance at once. Is the balance 20 higher afterwards?",
        "Each deposit reads the balance, adds 10 in the application and writes the sum back. If"
            + " both read the same balance, the second write replaces the first: both deposits"
            + " commit, both are recorded, and the balance shows only one of them. No error, no"
            + " warning; one customer's money silently disappears.",
        "final balance = initial balance + 10 × committed deposits",
        "Initial balance",
        List.of(
            mode(
                Mode.UNSAFE,
                "balance = read + 10, computed in the application and written as a value: the write"
                    + " does not know the row changed since it was read.",
                "SELECT balance FROM account WHERE id = ?;\n"
                    + "UPDATE account SET balance = :balance_read + 10 WHERE id = ?;",
                Isolation.READ_COMMITTED,
                false),
            mode(
                Mode.ATOMIC,
                "balance = balance + 10 inside the UPDATE. The second UPDATE waits for the first's"
                    + " row lock, then adds 10 to the committed balance, not to what it read.",
                "UPDATE account SET balance = balance + 10 WHERE id = ?;",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.PESSIMISTIC,
                "SELECT … FOR UPDATE: the second deposit cannot even read the balance until the"
                    + " first commits, so it reads the new balance.",
                "SELECT balance FROM account WHERE id = ? FOR UPDATE;\n"
                    + "UPDATE account SET balance = :balance_read + 10 WHERE id = ?;",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.OPTIMISTIC,
                "Write only if the version is still the one read. The loser's write matches no row;"
                    + " it retries on top of the new balance.",
                "UPDATE account SET balance = ?, version = :v + 1\n WHERE id = ? AND version = :v;",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.SERIALIZABLE,
                "The unsafe code at SERIALIZABLE: updating a row changed since the snapshot aborts"
                    + " with 40001, and the retry reads the new balance.",
                "BEGIN ISOLATION LEVEL SERIALIZABLE;\n-- unsafe SELECT and UPDATE\n-- 40001 → retry",
                Isolation.SERIALIZABLE,
                true)),
        Mode.UNSAFE,
        Isolation.READ_COMMITTED,
        new ExperimentInfo.Limits(2, 20, 2, 0, 100_000, 100, 150, 3),
        List.of(),
        "A read 100 · B read 100 · A wrote 110 · B wrote 110 · both committed → one deposit lost.");
  }

  @Override
  public Map<String, Object> seed(LabDatabase db, RunContext run) {
    long account =
        db.insert(
            "INSERT INTO account(run_id, name, balance, version) VALUES (?, 'Savings', ?, 1)"
                + " RETURNING id",
            run.runId(),
            run.config().initialValue());
    return Map.of("run", run.runId(), "account", account);
  }

  @Override
  public Map<String, Object> observe(LabDatabase db, RunContext run) {
    return db.one(
        "SELECT a.balance, a.version, (SELECT count(*) FROM deposit d WHERE d.account_id = a.id)"
            + " AS deposits FROM account a WHERE a.id = ?",
        run.key("account"));
  }

  @Override
  public String describe(Map<String, Object> state) {
    return "balance=" + number(state, "balance") + " · deposits=" + number(state, "deposits");
  }

  @Override
  String target(Participant p) {
    return "account #" + p.key("account");
  }

  @Override
  Read read(Participant p, boolean lock) {
    return Read.of(target(p), READ + (lock ? " FOR UPDATE" : ""), "balance", p.key("account"))
        .versioned("version");
  }

  @Override
  Decision decide(Participant p, Row row) {
    return null; // a deposit is always allowed
  }

  @Override
  Write blindWrite(Participant p, Row row) {
    long next = row.number("balance") + AMOUNT;
    return Write.update(
            target(p),
            "UPDATE account SET balance = ?, version = version + 1 WHERE id = ?",
            "balance=" + next,
            next,
            p.key("account"))
        .version(row.number("version") + 1);
  }

  @Override
  Write conditionalWrite(Participant p, Row row) {
    return Write.update(
        target(p),
        "UPDATE account SET balance = balance + " + AMOUNT + ", version = version + 1 WHERE id = ?",
        "balance=balance+" + AMOUNT,
        p.key("account"));
  }

  @Override
  Write versionedWrite(Participant p, Row row) {
    long next = row.number("balance") + AMOUNT;
    long v = row.number("version");
    return Write.update(
            target(p),
            "UPDATE account SET balance = ?, version = ? WHERE id = ? AND version = ?",
            "balance=" + next,
            next,
            v + 1,
            p.key("account"),
            v)
        .version(v + 1);
  }

  @Override
  String conditionFailed(Participant p) {
    return "the account disappeared";
  }

  @Override
  Write record(Participant p, Row row) {
    return Write.insert(
        "deposit",
        "INSERT INTO deposit(run_id, account_id, request_id, amount) VALUES (?, ?, ?, ?)",
        "deposit of " + AMOUNT,
        p.key("run"),
        p.key("account"),
        p.identity().requestId(),
        AMOUNT);
  }

  @Override
  String succeeded(Participant p, Row row) {
    return "deposited " + AMOUNT;
  }

  @Override
  public InvariantResult check(
      RunContext run,
      Map<String, Object> initial,
      Map<String, Object> result,
      List<RequestResult> requests,
      List<RaceEvent> events) {
    long start = run.config().initialValue();
    long deposits = number(result, "deposits");
    long balance = number(result, "balance");
    long expected = start + AMOUNT * deposits;
    boolean holds = balance == expected;
    return new InvariantResult(
        "final balance = initial balance + 10 × committed deposits",
        holds,
        "balance " + expected + " (" + start + " + " + plural(deposits, "deposit") + " of " + AMOUNT + ")",
        "balance " + balance + " after " + plural(deposits, "committed deposit"),
        holds
            ? "Every committed deposit is in the balance."
            : (expected - balance) + " of committed money is missing from the balance.");
  }

  @Override
  public String conclusion(RunContext run, InvariantResult invariant, List<RequestResult> requests) {
    if (invariant.holds()) return Texts.preserved(run.config(), requests);
    int ok = Texts.count(requests, Outcome.SUCCEEDED);
    return "The deposits read the same balance, each added "
        + AMOUNT
        + " to its own copy, and each wrote its sum. The later write replaced the earlier one:"
        + " "
        + plural(ok, "deposit")
        + " committed, but the balance moved as if fewer had. The deposit rows prove the money"
        + " arrived; the balance lost it.";
  }

  @Override
  public Explanation.Fix fix(RunConfig config, InvariantResult invariant) {
    if (invariant.holds()) return null;
    return Texts.fix(
        Mode.ATOMIC,
        Isolation.READ_COMMITTED,
        "Let the database do the arithmetic on the row it locks: balance = balance + 10.");
  }
}
