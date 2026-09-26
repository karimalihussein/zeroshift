package io.zeroshift.racelab.experiments;

import static io.zeroshift.racelab.experiments.Texts.number;

import io.zeroshift.racelab.application.Participant;
import io.zeroshift.racelab.application.Participant.Read;
import io.zeroshift.racelab.application.Participant.Write;
import io.zeroshift.racelab.application.port.LabDatabase;
import io.zeroshift.racelab.domain.ExperimentInfo;
import io.zeroshift.racelab.domain.ExperimentInfo.ModeInfo;
import io.zeroshift.racelab.domain.Isolation;
import io.zeroshift.racelab.domain.Mode;
import java.util.List;
import java.util.Map;

/** 9. A report counts rows matching a condition twice; a new matching row commits in between. */
public class PhantomRead extends ReadTwice {
  static final int AMOUNT = 25;

  @Override
  public ExperimentInfo info() {
    return new ExperimentInfo(
        "phantom-read",
        9,
        "Phantom read",
        "Isolation anomalies",
        "A statement counts the deposits of an account twice inside one transaction. Can a row"
            + " appear between the two counts?",
        "No existing row changes here: request B inserts a new deposit that matches A's WHERE"
            + " clause and commits between A's two counts. At READ COMMITTED the second count"
            + " includes the newcomer, a phantom. The SQL standard allows phantoms at REPEATABLE"
            + " READ; PostgreSQL's REPEATABLE READ uses a transaction-wide snapshot, so it shows"
            + " none. Try both.",
        "the same query returns the same rows twice in one transaction",
        "Existing deposits",
        List.of(
            new ModeInfo(
                Mode.UNSAFE,
                "Plain reads",
                "Two identical COUNT queries in one transaction, at the isolation level you choose.",
                "SELECT count(*) FROM deposit WHERE account_id = ?;\n"
                    + "-- B: INSERT INTO deposit …; COMMIT\n"
                    + "SELECT count(*) FROM deposit WHERE account_id = ?;",
                Isolation.READ_COMMITTED,
                false),
            new ModeInfo(
                Mode.SERIALIZABLE,
                "Serializable",
                "The same queries at SERIALIZABLE.",
                "BEGIN ISOLATION LEVEL SERIALIZABLE;",
                Isolation.SERIALIZABLE,
                true)),
        Mode.UNSAFE,
        Isolation.READ_COMMITTED,
        new ExperimentInfo.Limits(2, 2, 2, 0, 20, 3, 100, 0),
        List.of("reader", "writer"),
        "A counted 3 deposits · B inserted one and committed · A counted 4 → a phantom row.");
  }

  @Override
  public Map<String, Object> seed(LabDatabase db, RunContext run) {
    long account =
        db.insert(
            "INSERT INTO account(run_id, name, balance, version) VALUES (?, 'Savings', 0, 1)"
                + " RETURNING id",
            run.runId());
    for (int i = 0; i < run.config().initialValue(); i++)
      db.update(
          "INSERT INTO deposit(run_id, account_id, request_id, amount) VALUES (?, ?, 'seed', ?)",
          run.runId(),
          account,
          AMOUNT);
    return Map.of("run", run.runId(), "account", account);
  }

  @Override
  Read query(Participant p) {
    return Read.of(
        "deposits of account #" + p.key("account"),
        "SELECT count(*) AS deposits, coalesce(sum(amount), 0) AS total FROM deposit WHERE account_id = ?",
        "deposits",
        p.key("account"));
  }

  @Override
  Write change(Participant p) {
    return Write.insert(
        "deposits of account #" + p.key("account"),
        "INSERT INTO deposit(run_id, account_id, request_id, amount) VALUES (?, ?, ?, ?)",
        "new deposit of " + AMOUNT,
        p.key("run"),
        p.key("account"),
        p.identity().requestId(),
        AMOUNT);
  }

  @Override
  String changed() {
    return "inserted a deposit of " + AMOUNT;
  }

  @Override
  public Map<String, Object> observe(LabDatabase db, RunContext run) {
    return db.one(
        "SELECT count(*) AS deposits, coalesce(sum(amount), 0) AS total FROM deposit WHERE account_id = ?",
        run.key("account"));
  }

  @Override
  public String describe(Map<String, Object> state) {
    return "deposits=" + number(state, "deposits");
  }
}
