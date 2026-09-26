package io.zeroshift.racelab.experiments;

import static io.zeroshift.racelab.experiments.Texts.mode;
import static io.zeroshift.racelab.experiments.Texts.number;

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

/** 4. A shipping worker and a cancellation worker both move the same PAID order. */
public class StateTransition extends ReadDecideWrite {
  static final String READ = "SELECT status, version FROM orders WHERE id = ?";

  @Override
  public ExperimentInfo info() {
    return new ExperimentInfo(
        "state-transition",
        4,
        "Concurrent order state transition",
        "Check-then-act",
        "The warehouse ships an order while the customer cancels it. Can it be both shipped and"
            + " refunded?",
        "Both transitions are legal from PAID: PAID → SHIPPED and PAID → CANCELLED. Each worker"
            + " reads PAID, validates its transition, performs its side effect (a shipment or a"
            + " refund) and writes its status. If both validate before either writes, both side"
            + " effects happen and the last status written wins: a cancelled order that was shipped.",
        "exactly one transition out of PAID: one side effect, matching the final status",
        "",
        List.of(
            mode(
                Mode.UNSAFE,
                "Validate the transition against the status read, then write the new status. The"
                    + " write does not re-check the status it replaces.",
                "SELECT status FROM orders WHERE id = ?;\n-- if (status == PAID) …\n"
                    + "UPDATE orders SET status = :target WHERE id = ?;",
                Isolation.READ_COMMITTED,
                false),
            mode(
                Mode.ATOMIC,
                "A compare-and-set on the status: move to the target only if the order is still"
                    + " PAID. The loser matches no row and performs no side effect.",
                "UPDATE orders SET status = :target\n WHERE id = ? AND status = 'PAID';",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.PESSIMISTIC,
                "Lock the order while validating. The second worker waits, then reads the new status"
                    + " and refuses its transition.",
                "SELECT status FROM orders WHERE id = ? FOR UPDATE;",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.OPTIMISTIC,
                "Write the transition only at the version validated. The loser retries, reads the"
                    + " other status and refuses.",
                "UPDATE orders SET status = :target, version = :v + 1\n"
                    + " WHERE id = ? AND version = :v;",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.SERIALIZABLE,
                "The unsafe code at SERIALIZABLE: the second transition aborts with 40001; the retry"
                    + " sees the order already moved.",
                "BEGIN ISOLATION LEVEL SERIALIZABLE;\n-- unsafe code\n-- 40001 → retry",
                Isolation.SERIALIZABLE,
                true)),
        Mode.UNSAFE,
        Isolation.READ_COMMITTED,
        new ExperimentInfo.Limits(2, 10, 2, 1, 1, 1, 150, 3),
        List.of("ship", "cancel"),
        "A read PAID · B read PAID · A shipped · B refunded · status CANCELLED → shipped and"
            + " refunded.");
  }

  static String targetStatus(Participant p) {
    return "ship".equals(p.role()) ? "SHIPPED" : "CANCELLED";
  }

  static String effect(Participant p) {
    return "ship".equals(p.role()) ? "SHIPMENT" : "REFUND";
  }

  @Override
  public Map<String, Object> seed(LabDatabase db, RunContext run) {
    var first = run.requests().getFirst();
    var order = java.util.UUID.fromString(first.orderId());
    db.update(
        "INSERT INTO orders(id, run_id, customer_id, status, currency, total)"
            + " VALUES (?, ?, ?, 'PAID', 'USD', 89.00)",
        order,
        run.runId(),
        java.util.UUID.fromString(first.customerId()));
    return Map.of("run", run.runId(), "order", order);
  }

  @Override
  public Map<String, Object> observe(LabDatabase db, RunContext run) {
    return db.one(
        "SELECT o.status, o.version,"
            + " (SELECT count(*) FROM order_effect e WHERE e.order_id = o.id AND e.effect = 'SHIPMENT') AS shipments,"
            + " (SELECT count(*) FROM order_effect e WHERE e.order_id = o.id AND e.effect = 'REFUND') AS refunds"
            + " FROM orders o WHERE o.id = ?",
        run.uuid("order"));
  }

  @Override
  public String describe(Map<String, Object> state) {
    return "status="
        + state.get("status")
        + " · shipments="
        + number(state, "shipments")
        + " · refunds="
        + number(state, "refunds");
  }

  @Override
  String target(Participant p) {
    return "order " + p.uuid("order").toString().substring(0, 8);
  }

  @Override
  Read read(Participant p, boolean lock) {
    return Read.of(target(p), READ + (lock ? " FOR UPDATE" : ""), "status", p.uuid("order"))
        .versioned("version");
  }

  @Override
  Decision decide(Participant p, Row row) {
    var status = row.text("status");
    boolean paid = "PAID".equals(status);
    var move = status + " → " + targetStatus(p);
    return new Decision(
        paid,
        move + " allowed",
        paid
            ? "a PAID order may be " + targetStatus(p).toLowerCase()
            : "the order is already " + status,
        "refused " + move + ": the order is already " + status);
  }

  @Override
  Write blindWrite(Participant p, Row row) {
    return Write.update(
            target(p),
            "UPDATE orders SET status = ?, version = version + 1 WHERE id = ?",
            "status=" + targetStatus(p),
            targetStatus(p),
            p.uuid("order"))
        .version(row.number("version") + 1);
  }

  @Override
  Write conditionalWrite(Participant p, Row row) {
    return Write.update(
        target(p),
        "UPDATE orders SET status = ?, version = version + 1 WHERE id = ? AND status = 'PAID'",
        "status=" + targetStatus(p),
        targetStatus(p),
        p.uuid("order"));
  }

  @Override
  Write versionedWrite(Participant p, Row row) {
    long v = row.number("version");
    return Write.update(
            target(p),
            "UPDATE orders SET status = ?, version = ? WHERE id = ? AND version = ?",
            "status=" + targetStatus(p),
            targetStatus(p),
            v + 1,
            p.uuid("order"),
            v)
        .version(v + 1);
  }

  @Override
  String conditionFailed(Participant p) {
    return "refused: the order is no longer PAID (UPDATE … WHERE status = 'PAID' matched no row)";
  }

  @Override
  Write record(Participant p, Row row) {
    return Write.insert(
        "order effects",
        "INSERT INTO order_effect(run_id, order_id, effect, request_id) VALUES (?, ?, ?, ?)",
        effect(p),
        p.key("run"),
        p.uuid("order"),
        effect(p),
        p.identity().requestId());
  }

  @Override
  String succeeded(Participant p, Row row) {
    return "PAID → " + targetStatus(p) + " with a " + effect(p).toLowerCase();
  }

  @Override
  public InvariantResult check(
      RunContext run,
      Map<String, Object> initial,
      Map<String, Object> result,
      List<RequestResult> requests,
      List<RaceEvent> events) {
    long shipments = number(result, "shipments");
    long refunds = number(result, "refunds");
    var status = String.valueOf(result.get("status"));
    boolean holds =
        shipments + refunds == 1
            && (shipments == 1 ? status.equals("SHIPPED") : status.equals("CANCELLED"));
    return new InvariantResult(
        "exactly one transition out of PAID: one side effect, matching the final status",
        holds,
        "SHIPPED with 1 shipment, or CANCELLED with 1 refund",
        status + " with " + shipments + " shipment(s) and " + refunds + " refund(s)",
        holds
            ? "One transition won; the other worker saw the order had moved on."
            : shipments > 0 && refunds > 0
                ? "The goods left the warehouse and the money went back: both transitions ran."
                : "The status does not match the side effect performed.");
  }

  @Override
  public String conclusion(
      RunContext run, InvariantResult invariant, List<RequestResult> requests) {
    if (invariant.holds()) return Texts.preserved(run.config(), requests);
    return "Both workers validated their transition against PAID, the only state they had read."
        + " Each then performed its side effect and wrote its status; the last write decided the"
        + " status, but nothing undid the other side effect.";
  }

  @Override
  public Explanation.Fix fix(RunConfig config, InvariantResult invariant) {
    if (invariant.holds()) return null;
    return Texts.fix(
        Mode.ATOMIC,
        Isolation.READ_COMMITTED,
        "Make the transition a compare-and-set: UPDATE … WHERE status = 'PAID'. Perform the side"
            + " effect only when your UPDATE matched the row.");
  }
}
