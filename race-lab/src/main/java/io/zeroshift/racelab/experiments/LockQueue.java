package io.zeroshift.racelab.experiments;

import static io.zeroshift.racelab.experiments.Texts.mode;

import io.zeroshift.racelab.domain.ExperimentInfo;
import io.zeroshift.racelab.domain.Isolation;
import io.zeroshift.racelab.domain.Mode;
import io.zeroshift.racelab.domain.RunConfig;
import io.zeroshift.racelab.domain.RunResult.Explanation;
import io.zeroshift.racelab.domain.RunResult.InvariantResult;
import java.util.List;

/**
 * 6. A flash sale with plenty of stock: every reservation is legitimate, so the lesson is the cost
 * of correctness. Under a pessimistic lock the requests queue on one row, each holding the lock
 * through its application work; the atomic update holds it only for one statement.
 */
public class LockQueue extends Oversell {
  @Override
  public ExperimentInfo info() {
    return new ExperimentInfo(
        "pessimistic-locking",
        6,
        "Pessimistic locking: the lock queue",
        "Locks",
        "Ten buyers, a hundred items: nobody should be refused. What does the lock cost each of"
            + " them?",
        "SELECT … FOR UPDATE makes the check-then-act safe by making it exclusive: the first"
            + " request holds the row lock through its read, its application work and its commit,"
            + " and every other request waits in PostgreSQL's lock queue for its turn. Watch the"
            + " waits stack up: the last request pays for everyone before it. Then run the same"
            + " sale with an atomic update (the lock lasts one statement) or optimistic locking (no"
            + " lock, conflicts and retries instead).",
        "every reservation is counted: final stock = initial stock − reservations",
        "Initial stock",
        List.of(
            mode(
                Mode.PESSIMISTIC,
                "Each request locks the row with SELECT … FOR UPDATE and keeps it through its"
                    + " application work until COMMIT. The others block in their SELECT, one"
                    + " behind the other.",
                "SELECT stock FROM item WHERE id = ? FOR UPDATE;\n-- application work (delay)\n"
                    + "UPDATE item SET stock = :stock_read - 1 WHERE id = ?;\nCOMMIT;",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.ATOMIC,
                "The application work happens before the UPDATE, without a lock. The row is locked"
                    + " only from the UPDATE to COMMIT, so waits are short.",
                "-- application work (delay), no lock\n"
                    + "UPDATE item SET stock = stock - 1 WHERE id = ? AND stock > 0;",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.OPTIMISTIC,
                "No lock while working. Everyone reads the same version; one write wins per round,"
                    + " the rest conflict and retry: waits become retries.",
                "UPDATE item SET stock = ?, version = :v + 1\n WHERE id = ? AND version = :v;",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.UNSAFE,
                "No lock, no check at write time: fast, and every reservation after the first"
                    + " overwrites the stock computed by the others.",
                "SELECT stock FROM item WHERE id = ?;\n"
                    + "UPDATE item SET stock = :stock_read - 1 WHERE id = ?;",
                Isolation.READ_COMMITTED,
                false)),
        Mode.PESSIMISTIC,
        Isolation.READ_COMMITTED,
        new ExperimentInfo.Limits(2, 20, 5, 1, 1000, 100, 200, 10),
        List.of(),
        "A locks · B, C, D, E wait · A commits · B wakes · … each request waits for all before it.");
  }

  @Override
  public Explanation.Fix fix(RunConfig config, InvariantResult invariant) {
    if (!invariant.holds()) return super.fix(config, invariant);
    if (config.mode() == Mode.PESSIMISTIC)
      return Texts.fix(
          Mode.ATOMIC,
          Isolation.READ_COMMITTED,
          "Correct, but slow under contention: every request waited for the ones before it. The"
              + " atomic update keeps correctness and holds the lock for one statement only.");
    return null;
  }
}
