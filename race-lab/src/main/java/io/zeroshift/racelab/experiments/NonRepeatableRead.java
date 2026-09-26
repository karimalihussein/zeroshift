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

/** 8. A report reads a balance twice in one transaction; a deposit commits in between. */
public class NonRepeatableRead extends ReadTwice {
  static final int DEPOSIT = 50;

  @Override
  public ExperimentInfo info() {
    return new ExperimentInfo(
        "non-repeatable-read",
        8,
        "Non-repeatable read",
        "Isolation anomalies",
        "A report reads the same balance twice inside one transaction. Can it get two different"
            + " answers?",
        "Request A (the reader) reads the balance, does some work, and reads it again. Request B"
            + " (the writer) deposits "
            + DEPOSIT
            + " and commits between A's two reads. At READ"
            + " COMMITTED each statement sees whatever is committed when it starts, so A's second"
            + " read differs from its first. At REPEATABLE READ A sees one snapshot for its whole"
            + " transaction. Change the isolation level and run it again.",
        "two reads of the same row in one transaction return the same value",
        "Initial balance",
        List.of(
            new ModeInfo(
                Mode.UNSAFE,
                "Plain reads",
                "Two plain SELECTs inside one transaction, at the isolation level you choose.",
                "BEGIN ISOLATION LEVEL :level;\nSELECT balance FROM account WHERE id = ?;\n"
                    + "-- B: UPDATE … balance + 50; COMMIT\nSELECT balance FROM account WHERE id = ?;",
                Isolation.READ_COMMITTED,
                false),
            new ModeInfo(
                Mode.SERIALIZABLE,
                "Serializable",
                "The same reads at SERIALIZABLE: one snapshot, like REPEATABLE READ.",
                "BEGIN ISOLATION LEVEL SERIALIZABLE;",
                Isolation.SERIALIZABLE,
                true)),
        Mode.UNSAFE,
        Isolation.READ_COMMITTED,
        new ExperimentInfo.Limits(2, 2, 2, 0, 100_000, 100, 100, 0),
        List.of("reader", "writer"),
        "A read 100 · B deposited 50 and committed · A read 150 → one transaction, two answers.");
  }

  @Override
  public Map<String, Object> seed(LabDatabase db, RunContext run) {
    long account =
        db.insert(
            "INSERT INTO account(run_id, name, balance, version) VALUES (?, 'Checking', ?, 1)"
                + " RETURNING id",
            run.runId(),
            run.config().initialValue());
    return Map.of("run", run.runId(), "account", account);
  }

  @Override
  Read query(Participant p) {
    return Read.of(
            "account #" + p.key("account"),
            "SELECT balance, version FROM account WHERE id = ?",
            "balance",
            p.key("account"))
        .versioned("version");
  }

  @Override
  Write change(Participant p) {
    return Write.update(
        "account #" + p.key("account"),
        "UPDATE account SET balance = balance + "
            + DEPOSIT
            + ", version = version + 1 WHERE id = ?",
        "balance=balance+" + DEPOSIT,
        p.key("account"));
  }

  @Override
  String changed() {
    return "deposited " + DEPOSIT;
  }

  @Override
  public Map<String, Object> observe(LabDatabase db, RunContext run) {
    return db.one("SELECT balance, version FROM account WHERE id = ?", run.key("account"));
  }

  @Override
  public String describe(Map<String, Object> state) {
    return "balance=" + number(state, "balance");
  }
}
