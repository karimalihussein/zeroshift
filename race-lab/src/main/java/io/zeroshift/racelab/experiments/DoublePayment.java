package io.zeroshift.racelab.experiments;

import static io.zeroshift.racelab.experiments.Texts.mode;
import static io.zeroshift.racelab.experiments.Texts.number;
import static io.zeroshift.racelab.experiments.Texts.plural;

import io.zeroshift.contracts.Money;
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
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 3. The same order paid twice: if (!paid) charge(). Uses the commerce model's orders and payment
 * tables; the fix that payment-service itself uses is a unique idempotency key per order.
 */
public class DoublePayment extends ReadDecideWrite {
  static final String READ = "SELECT status, version FROM orders WHERE id = ?";

  @Override
  public ExperimentInfo info() {
    return new ExperimentInfo(
        "double-payment",
        3,
        "Double payment",
        "Check-then-act",
        "A double-click, a client retry or a redelivered message: two workers pay the same order."
            + " How many times is the card charged?",
        "Every worker runs if (!paid) charge(). Both read status = PLACED, both conclude nobody"
            + " has charged yet, both record a payment and mark the order PAID. The status ends up"
            + " correct and hides that the customer paid twice. Each payment row is a charge"
            + " attempt with its own idempotency key; the unsafe workers each invent one.",
        "at most one payment per order",
        "Order total ($)",
        List.of(
            mode(
                Mode.UNSAFE,
                "Read the order's status, check it in the application, charge under a key of this"
                    + " request's own, then set PAID. Two workers can pass the check before either"
                    + " sets PAID, and their keys never collide.",
                "SELECT status FROM orders WHERE id = ?;\n-- if (status == PLACED) charge()\n"
                    + "UPDATE orders SET status = 'PAID' WHERE id = ?;\n"
                    + "INSERT INTO payment(idempotency_key, …) VALUES ('req:…', …);",
                Isolation.READ_COMMITTED,
                false),
            mode(
                Mode.ATOMIC,
                "Charge under the order's idempotency key, as payment-service does: the payment row"
                    + " is inserted only if the key is new. A second worker's INSERT waits on the"
                    + " first one's uncommitted key, then inserts nothing: the duplicate is"
                    + " suppressed by the unique index, not by a check in Java.",
                "INSERT INTO payment(idempotency_key, …)\n VALUES ('order:<id>:authorize', …)\n"
                    + " ON CONFLICT (idempotency_key) DO NOTHING;\n-- 1 row: charged · 0 rows: duplicate",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.PESSIMISTIC,
                "Lock the order row while checking it. The second worker waits, then reads PAID"
                    + " and skips.",
                "SELECT status FROM orders WHERE id = ? FOR UPDATE;",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.OPTIMISTIC,
                "Mark PAID only if the order's version is unchanged. The late worker's UPDATE matches"
                    + " no row; its retry reads PAID and skips.",
                "UPDATE orders SET status = 'PAID', version = :v + 1\n WHERE id = ? AND version = :v;",
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
        "A read PLACED · B read PLACED · A charged · B charged · both set PAID → charged twice.");
  }

  @Override
  public Map<String, Object> seed(LabDatabase db, RunContext run) {
    var first = run.requests().getFirst();
    var order = UUID.fromString(first.orderId());
    db.update(
        "INSERT INTO orders(id, run_id, customer_id, status, currency, total) VALUES (?, ?, ?, 'PLACED', 'USD', ?)",
        order,
        run.runId(),
        UUID.fromString(first.customerId()),
        Money.of(new BigDecimal(run.config().initialValue())));
    return Map.of("run", run.runId(), "order", order);
  }

  @Override
  public Map<String, Object> observe(LabDatabase db, RunContext run) {
    return db.one(
        "SELECT o.status, o.version, o.total,"
            + " (SELECT count(*) FROM payment p WHERE p.order_id = o.id) AS payments,"
            + " (SELECT coalesce(sum(p.amount), 0) FROM payment p WHERE p.order_id = o.id) AS charged"
            + " FROM orders o WHERE o.id = ?",
        run.uuid("order"));
  }

  @Override
  public String describe(Map<String, Object> state) {
    return "status=" + state.get("status") + " · payments=" + number(state, "payments");
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
    boolean placed = "PLACED".equals(status);
    return new Decision(
        placed,
        "status = PLACED",
        placed ? "not paid yet: charge the card" : "already " + status + ": nothing to do",
        "duplicate: the order is already " + status);
  }

  @Override
  Write blindWrite(Participant p, Row row) {
    return Write.update(
            target(p),
            "UPDATE orders SET status = 'PAID', version = version + 1 WHERE id = ?",
            "status=PAID",
            p.uuid("order"))
        .version(row.number("version") + 1);
  }

  /** The order's own key: the same key payment-service uses for the saga's authorization. */
  static String orderKey(Participant p) {
    return "order:" + p.uuid("order") + ":authorize";
  }

  @Override
  Write conditionalWrite(Participant p, Row row) {
    return new Write(
        "payment key " + orderKey(p).substring(0, 14) + "…",
        "INSERT INTO payment(run_id, order_id, idempotency_key, status, amount, currency, request_id, authorized_at)"
            + " SELECT ?, id, ?, 'AUTHORIZED', total, currency, ?, clock_timestamp() FROM orders WHERE id = ?"
            + " ON CONFLICT (idempotency_key) DO NOTHING",
        List.of(p.key("run"), orderKey(p), p.identity().requestId(), p.uuid("order")),
        "payment by " + p.lane(),
        null,
        "unique key " + orderKey(p));
  }

  @Override
  Write versionedWrite(Participant p, Row row) {
    long v = row.number("version");
    return Write.update(
            target(p),
            "UPDATE orders SET status = 'PAID', version = ? WHERE id = ? AND version = ?",
            "status=PAID",
            v + 1,
            p.uuid("order"),
            v)
        .version(v + 1);
  }

  @Override
  String conditionFailed(Participant p) {
    return "duplicate: key "
        + orderKey(p)
        + " already has a payment (ON CONFLICT DO NOTHING inserted no row)";
  }

  /**
   * After the order's claim: the atomic mode charged first, so it now marks the order paid; every
   * other mode marked it paid and now charges, under a key only this request uses.
   */
  @Override
  Write record(Participant p, Row row) {
    if (p.mode() == Mode.ATOMIC)
      return Write.update(
          target(p),
          "UPDATE orders SET status = 'PAID', version = version + 1 WHERE id = ?",
          "status=PAID",
          p.uuid("order"));
    return Write.insert(
        "payments",
        "INSERT INTO payment(run_id, order_id, idempotency_key, status, amount, currency, request_id, authorized_at)"
            + " SELECT ?, id, ?, 'AUTHORIZED', total, currency, ?, clock_timestamp() FROM orders WHERE id = ?",
        "payment by " + p.lane(),
        p.key("run"),
        "req:" + p.identity().requestId(),
        p.identity().requestId(),
        p.uuid("order"));
  }

  @Override
  String succeeded(Participant p, Row row) {
    return "charged the card and marked the order PAID";
  }

  @Override
  public InvariantResult check(
      RunContext run,
      Map<String, Object> initial,
      Map<String, Object> result,
      List<RequestResult> requests,
      List<RaceEvent> events) {
    long payments = number(result, "payments");
    boolean holds = payments <= 1;
    return new InvariantResult(
        info().invariant(),
        holds,
        "at most 1 payment of $" + run.config().initialValue(),
        plural(payments, "payment")
            + ", $"
            + result.get("charged")
            + " charged, order "
            + result.get("status"),
        holds
            ? "One payment; every other worker recognised the order as paid."
            : "The customer was charged " + payments + " times for one order.");
  }

  @Override
  public String conclusion(
      RunContext run, InvariantResult invariant, List<RequestResult> requests) {
    if (invariant.holds()) return Texts.preserved(run.config(), requests);
    return "Each worker checked status = PLACED before any of them had written PAID, so each"
        + " believed it was first, and each charged under a key of its own. The check and the"
        + " charge were two steps with a gap between them, and a second worker walked through the"
        + " gap.";
  }

  @Override
  public Explanation.Fix fix(RunConfig config, InvariantResult invariant) {
    if (invariant.holds()) return null;
    return Texts.fix(
        Mode.ATOMIC,
        Isolation.READ_COMMITTED,
        "Charge under one idempotency key per order, claimed by the database: INSERT … ON CONFLICT"
            + " (idempotency_key) DO NOTHING. Only the worker whose row was inserted has charged.");
  }
}
