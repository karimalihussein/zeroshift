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
import io.zeroshift.racelab.domain.RunResult.RequestResult;
import java.util.List;
import java.util.Map;

/** 3. The same payment processed twice: if (!processed) processPayment(). */
public class DoublePayment extends ReadDecideWrite {
  static final String READ = "SELECT status, version FROM payment WHERE id = ?";

  @Override
  public ExperimentInfo info() {
    return new ExperimentInfo(
        "double-payment",
        3,
        "Double payment",
        "Check-then-act",
        "A double-click, a client retry or a redelivered message: two workers process the same"
            + " payment. How many times is the card charged?",
        "Every worker runs if (!processed) processPayment(). Both read status = PENDING, both"
            + " conclude nobody has charged yet, both call the gateway and record a charge, both mark"
            + " the payment PAID. The status ends up correct and hides that the customer paid"
            + " twice.",
        "at most one charge per payment",
        "Amount (€)",
        List.of(
            mode(
                Mode.UNSAFE,
                "Read the status, check it in the application, charge, then set PAID. Two workers can"
                    + " pass the check before either sets PAID.",
                "SELECT status FROM payment WHERE id = ?;\n-- if (status == PENDING) charge()\n"
                    + "UPDATE payment SET status = 'PAID' WHERE id = ?;\nINSERT INTO charge …;",
                Isolation.READ_COMMITTED,
                false),
            mode(
                Mode.ATOMIC,
                "Claim the payment in one statement: set PAID only where it is still PENDING. Exactly"
                    + " one worker's UPDATE matches the row; the other matches nothing and skips the"
                    + " charge as a duplicate.",
                "UPDATE payment SET status = 'PAID'\n WHERE id = ? AND status = 'PENDING';\n"
                    + "-- 1 row: charge · 0 rows: duplicate, skip",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.PESSIMISTIC,
                "Lock the payment row while checking it. The second worker waits, then reads PAID"
                    + " and skips.",
                "SELECT status FROM payment WHERE id = ? FOR UPDATE;",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.OPTIMISTIC,
                "Mark PAID only if the version is unchanged. The late worker's UPDATE matches no"
                    + " row; its retry reads PAID and skips.",
                "UPDATE payment SET status = 'PAID', version = :v + 1\n WHERE id = ? AND version = :v;",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.SERIALIZABLE,
                "The unsafe code at SERIALIZABLE: the second worker's update aborts with 40001 and"
                    + " its retry reads PAID.",
                "BEGIN ISOLATION LEVEL SERIALIZABLE;\n-- unsafe code\n-- 40001 → retry",
                Isolation.SERIALIZABLE,
                true)),
        Mode.UNSAFE,
        Isolation.READ_COMMITTED,
        new ExperimentInfo.Limits(2, 20, 2, 1, 10_000, 50, 150, 3),
        List.of(),
        "A read PENDING · B read PENDING · A charged · B charged · both set PAID → charged twice.");
  }

  @Override
  public Map<String, Object> seed(LabDatabase db, RunContext run) {
    var order = run.requests().getFirst().orderId();
    long payment =
        db.insert(
            "INSERT INTO payment(run_id, order_id, amount, status, version)"
                + " VALUES (?, ?, ?, 'PENDING', 1) RETURNING id",
            run.runId(),
            order,
            run.config().initialValue() * 100L);
    return Map.of("run", run.runId(), "payment", payment, "order", order);
  }

  @Override
  public Map<String, Object> observe(LabDatabase db, RunContext run) {
    return db.one(
        "SELECT p.status, p.version, p.amount,"
            + " (SELECT count(*) FROM charge c WHERE c.payment_id = p.id) AS charges,"
            + " (SELECT coalesce(sum(c.amount), 0) FROM charge c WHERE c.payment_id = p.id) AS charged"
            + " FROM payment p WHERE p.id = ?",
        run.key("payment"));
  }

  @Override
  public String describe(Map<String, Object> state) {
    return "status=" + state.get("status") + " · charges=" + number(state, "charges");
  }

  @Override
  String target(Participant p) {
    return "payment #" + p.key("payment");
  }

  @Override
  Read read(Participant p, boolean lock) {
    return Read.of(target(p), READ + (lock ? " FOR UPDATE" : ""), "status", p.key("payment"))
        .versioned("version");
  }

  @Override
  Decision decide(Participant p, Row row) {
    var status = row.text("status");
    boolean pending = "PENDING".equals(status);
    return new Decision(
        pending,
        "status = PENDING",
        pending ? "not processed yet: charge the card" : "already " + status + ": nothing to do",
        "duplicate: payment already " + status);
  }

  @Override
  Write blindWrite(Participant p, Row row) {
    return Write.update(
            target(p),
            "UPDATE payment SET status = 'PAID', version = version + 1 WHERE id = ?",
            "status=PAID",
            p.key("payment"))
        .version(row.number("version") + 1);
  }

  @Override
  Write conditionalWrite(Participant p, Row row) {
    return Write.update(
        target(p),
        "UPDATE payment SET status = 'PAID', version = version + 1 WHERE id = ? AND status = 'PENDING'",
        "status=PAID",
        p.key("payment"));
  }

  @Override
  Write versionedWrite(Participant p, Row row) {
    long v = row.number("version");
    return Write.update(
            target(p),
            "UPDATE payment SET status = 'PAID', version = ? WHERE id = ? AND version = ?",
            "status=PAID",
            v + 1,
            p.key("payment"),
            v)
        .version(v + 1);
  }

  @Override
  String conditionFailed(Participant p) {
    return "duplicate: UPDATE … WHERE status = 'PENDING' matched no row";
  }

  @Override
  Write record(Participant p, Row row) {
    return Write.insert(
        "charge",
        "INSERT INTO charge(run_id, payment_id, request_id, amount)"
            + " SELECT ?, id, ?, amount FROM payment WHERE id = ?",
        "charge by " + p.lane(),
        p.key("run"),
        p.identity().requestId(),
        p.key("payment"));
  }

  @Override
  String succeeded(Participant p, Row row) {
    return "charged the card and marked the payment PAID";
  }

  @Override
  public InvariantResult check(
      RunContext run,
      Map<String, Object> initial,
      Map<String, Object> result,
      List<RequestResult> requests,
      List<RaceEvent> events) {
    long charges = number(result, "charges");
    boolean holds = charges <= 1;
    return new InvariantResult(
        "at most one charge per payment",
        holds,
        "at most 1 charge of €" + run.config().initialValue(),
        plural(charges, "charge") + ", €" + number(result, "charged") / 100 + " taken, status " + result.get("status"),
        holds
            ? "One charge; every other worker recognised the payment as done."
            : "The customer was charged " + charges + " times for one payment.");
  }

  @Override
  public String conclusion(RunContext run, InvariantResult invariant, List<RequestResult> requests) {
    if (invariant.holds()) return Texts.preserved(run.config(), requests);
    return "Each worker checked status = PENDING before any of them had written PAID, so each"
        + " believed it was first. The check and the charge were two steps with a gap between them,"
        + " and a second worker walked through the gap.";
  }

  @Override
  public Explanation.Fix fix(RunConfig config, InvariantResult invariant) {
    if (invariant.holds()) return null;
    return Texts.fix(
        Mode.ATOMIC,
        Isolation.READ_COMMITTED,
        "Claim the work atomically: UPDATE … SET status = 'PAID' WHERE status = 'PENDING'. Only the"
            + " worker whose UPDATE matched a row may charge.");
  }
}
