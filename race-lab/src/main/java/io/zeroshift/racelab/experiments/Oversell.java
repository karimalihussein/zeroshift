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

/** 1. Several customers reserve the last item at once. */
public class Oversell extends ReadDecideWrite {
  static final String READ = "SELECT stock, version FROM item WHERE id = ?";

  @Override
  public ExperimentInfo info() {
    return new ExperimentInfo(
        "oversell",
        1,
        "Overselling inventory",
        "Check-then-act",
        "Two customers reserve the last item at the same moment. How many reservations succeed?",
        "Each request reads the stock, checks in Java that it is above zero, then writes back the"
            + " stock minus one. Between the check and the write, another request can read the same"
            + " stock and reach the same decision. Both write, both commit, and one item is sold"
            + " twice. The database did exactly what each statement asked.",
        "successful reservations ≤ available stock",
        "Initial stock",
        List.of(
            mode(
                Mode.UNSAFE,
                "Each request reads the stock, checks stock > 0 in the application, and writes back"
                    + " the value it computed from its own read. Nothing ties the write to what was"
                    + " read.",
                "SELECT stock FROM item WHERE id = ?;\n-- application: if (stock > 0) …\n"
                    + "UPDATE item SET stock = :stock_read - 1 WHERE id = ?;\n"
                    + "INSERT INTO reservation …;",
                Isolation.READ_COMMITTED,
                false),
            mode(
                Mode.ATOMIC,
                "The check moves into the UPDATE's WHERE clause. A second UPDATE waits for the first"
                    + " one's row lock, then PostgreSQL re-evaluates WHERE stock > 0 against the"
                    + " newly committed row (READ COMMITTED) and matches nothing: sold out.",
                "UPDATE item SET stock = stock - 1\n WHERE id = ? AND stock > 0;\n"
                    + "-- 1 row: reserved · 0 rows: sold out",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.PESSIMISTIC,
                "SELECT … FOR UPDATE locks the row as it reads it. The next request's SELECT blocks"
                    + " inside PostgreSQL until the first commits, then reads the new stock and"
                    + " declines. Correct, but every request waits in line.",
                "SELECT stock FROM item WHERE id = ? FOR UPDATE;\n-- others block here\n"
                    + "UPDATE item SET stock = :stock_read - 1 WHERE id = ?;",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.OPTIMISTIC,
                "No lock while thinking: each request remembers the version it read and writes only"
                    + " if it is unchanged. The loser's UPDATE matches no row; it rolls back and"
                    + " retries from a fresh read, which now shows the new stock.",
                "SELECT stock, version FROM item WHERE id = ?;\n"
                    + "UPDATE item SET stock = ?, version = :v + 1\n WHERE id = ? AND version = :v;\n"
                    + "-- 0 rows: someone was first → retry",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.SERIALIZABLE,
                "The unsafe code, unchanged, at SERIALIZABLE. When a transaction updates a row that"
                    + " changed after its snapshot, PostgreSQL aborts it with SQLSTATE 40001. The"
                    + " application must retry; the retry reads the new stock.",
                "BEGIN ISOLATION LEVEL SERIALIZABLE;\n-- same SELECT and UPDATE as unsafe\n"
                    + "-- ERROR 40001: could not serialize access → retry",
                Isolation.SERIALIZABLE,
                true)),
        Mode.UNSAFE,
        Isolation.READ_COMMITTED,
        limits(),
        List.of(),
        "A read stock=1 · B read stock=1 · A decided available · B decided available · A committed"
            + " · B committed → two reservations for one item.");
  }

  ExperimentInfo.Limits limits() {
    return new ExperimentInfo.Limits(2, 20, 2, 1, 50, 1, 150, 3);
  }

  @Override
  public Map<String, Object> seed(LabDatabase db, RunContext run) {
    var sku = "SKU-" + java.util.HexFormat.of().withUpperCase().toHexDigits((short) java.util.concurrent.ThreadLocalRandom.current().nextInt());
    long item =
        db.insert(
            "INSERT INTO item(run_id, sku, stock, version) VALUES (?, ?, ?, 1) RETURNING id",
            run.runId(),
            sku,
            run.config().initialValue());
    return Map.of("run", run.runId(), "item", item, "sku", sku);
  }

  @Override
  public Map<String, Object> observe(LabDatabase db, RunContext run) {
    return db.one(
        "SELECT i.stock, i.version, (SELECT count(*) FROM reservation r WHERE r.item_id = i.id)"
            + " AS reservations FROM item i WHERE i.id = ?",
        run.key("item"));
  }

  @Override
  public String describe(Map<String, Object> state) {
    return "stock=" + number(state, "stock") + " · reservations=" + number(state, "reservations");
  }

  @Override
  String target(Participant p) {
    return "item #" + p.key("item");
  }

  @Override
  Read read(Participant p, boolean lock) {
    return Read.of(target(p), READ + (lock ? " FOR UPDATE" : ""), "stock", p.key("item"))
        .versioned("version");
  }

  @Override
  Decision decide(Participant p, Row row) {
    int stock = row.integer("stock");
    return new Decision(
        stock > 0,
        "stock > 0",
        stock > 0 ? "stock=" + stock + ": inventory available" : "stock=" + stock + ": sold out",
        "sold out: read stock=" + stock);
  }

  @Override
  Write blindWrite(Participant p, Row row) {
    int next = row.integer("stock") - 1;
    return Write.update(
            target(p),
            "UPDATE item SET stock = ?, version = version + 1 WHERE id = ?",
            "stock=" + next,
            next,
            p.key("item"))
        .version(row.number("version") + 1);
  }

  @Override
  Write conditionalWrite(Participant p, Row row) {
    return Write.update(
        target(p),
        "UPDATE item SET stock = stock - 1, version = version + 1 WHERE id = ? AND stock > 0",
        "stock=stock-1",
        p.key("item"));
  }

  @Override
  Write versionedWrite(Participant p, Row row) {
    int next = row.integer("stock") - 1;
    long v = row.number("version");
    return Write.update(
            target(p),
            "UPDATE item SET stock = ?, version = ? WHERE id = ? AND version = ?",
            "stock=" + next,
            next,
            v + 1,
            p.key("item"),
            v)
        .version(v + 1);
  }

  @Override
  String conditionFailed(Participant p) {
    return "sold out: UPDATE … WHERE stock > 0 matched no row";
  }

  @Override
  Write record(Participant p, Row row) {
    var id = p.identity();
    return Write.insert(
        "reservation",
        "INSERT INTO reservation(run_id, item_id, request_id, order_id, customer_id)"
            + " VALUES (?, ?, ?, ?, ?)",
        "reservation for " + id.orderId(),
        p.key("run"),
        p.key("item"),
        id.requestId(),
        id.orderId(),
        id.customerId());
  }

  @Override
  String succeeded(Participant p, Row row) {
    return "reserved 1 item for " + p.identity().orderId();
  }

  @Override
  public InvariantResult check(
      RunContext run,
      Map<String, Object> initial,
      Map<String, Object> result,
      List<RequestResult> requests,
      List<RaceEvent> events) {
    long available = run.config().initialValue();
    long reservations = number(result, "reservations");
    long stock = number(result, "stock");
    boolean holds = reservations <= available && stock == available - reservations && stock >= 0;
    return new InvariantResult(
        info().invariant(),
        holds,
        "at most " + plural(available, "reservation") + ", final stock = " + available + " − reservations",
        plural(reservations, "reservation") + " for " + available + " in stock, final stock " + stock,
        holds
            ? "Every reservation matches one unit of stock."
            : (reservations - available)
                + " more reservation(s) than stock, and the stock column ("
                + stock
                + ") no longer counts the reservations made ("
                + reservations
                + ").");
  }

  @Override
  public String conclusion(RunContext run, InvariantResult invariant, List<RequestResult> requests) {
    int ok = Texts.count(requests, Outcome.SUCCEEDED);
    if (invariant.holds()) return Texts.preserved(run.config(), requests);
    return "Every successful request read the stock before any of them committed, each decided an"
        + " item was available, and each wrote the stock it computed from its own read. "
        + plural(ok, "reservation")
        + " committed for "
        + run.config().initialValue()
        + " in stock. No statement failed: the bug is the gap between the check and the write.";
  }

  @Override
  public Explanation.Fix fix(RunConfig config, InvariantResult invariant) {
    if (invariant.holds()) return null;
    return Texts.fix(
        Mode.ATOMIC,
        Isolation.READ_COMMITTED,
        "Put the check inside the write: UPDATE … WHERE stock > 0 is re-evaluated on the locked row,"
            + " so the second request sees the first one's commit.");
  }
}
