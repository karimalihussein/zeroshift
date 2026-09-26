package io.zeroshift.failures;

import static io.zeroshift.failures.Checks.check;
import static io.zeroshift.failures.Checks.compare;
import static io.zeroshift.failures.TwoPhaseSchema.*;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Two-phase commit versus a saga, on the same checkout (charge a payment, take stock) and the same
 * crash: the coordinator is SIGKILLed right after the participants did their work. With 2PC the
 * work is prepared, in doubt and locked until an operator resolves it; with the saga it is
 * committed, unlocked and visibly half-done until a new coordinator compensates it.
 */
@Component
public class TwoPhaseLab implements FailureLab {
  static final int LOCK_TIMEOUT_MS = 4000;
  static final String STOCK_ROW =
      "SELECT xmin::text AS xmin, xmax::text AS xmax, sku, on_hand FROM stock WHERE sku = ?";
  static final String ACCOUNT_ROW =
      "SELECT xmin::text AS xmin, xmax::text AS xmax, id, balance FROM account WHERE id = ?";

  private final FailureDb db;
  private final PostgresInspector inspector;
  private final List<CoordinatorProcess> processes = new CopyOnWriteArrayList<>();
  private final JsonMapper json = JsonMapper.builder().build();

  public TwoPhaseLab(FailureDb db) {
    this.db = db;
    this.inspector = new PostgresInspector(db);
  }

  @Override
  public String id() {
    return "two-phase";
  }

  @Override
  public String title() {
    return "2PC vs saga";
  }

  @Override
  public String summary() {
    return "One checkout charges fl_payments and takes stock in fl_inventory. The coordinator, a"
        + " separate JVM, is killed with SIGKILL right after both participants did their work:"
        + " once under two-phase commit (PREPARE TRANSACTION), once as a saga.";
  }

  @Override
  public String naive() {
    return "Two-phase commit: atomic and isolated, but a prepared participant cannot decide alone."
        + " When the coordinator dies after PREPARE, the work is in doubt and keeps its locks.";
  }

  @Override
  public String correct() {
    return "A saga: each step commits locally and the coordinator's progress is a durable log. A"
        + " crash leaves no locks, only a visible half-done order that a new coordinator"
        + " compensates.";
  }

  @Override
  public List<String> plan() {
    return List.of(
        "Recreate fl_payments, fl_inventory and fl_coordinator, then run one 2PC checkout to the end: both participants PREPARE (vote yes), the coordinator logs COMMIT, then COMMIT PREPARED on each.",
        "Start a second 2PC checkout, let both participants PREPARE, and SIGKILL the coordinator process before it writes a decision. Read pg_prepared_xacts: the work outlived its process and sessions.",
        "Measure the damage: the locks the prepared transactions hold (pg_locks, the row's xmax), a checkout and a payment on the same rows blocked until their lock_timeout (pg_blocking_pids = 0: a prepared transaction), a different SKU unaffected, readers seeing the old values, no session left that could release anything, and VACUUM unable to remove dead rows.",
        "Run the same checkout as a saga on an identical account and SKU and SIGKILL its coordinator at the same point, after the payment step. Nothing is prepared or locked: the same probes finish at once, but the half-done order (paid, no stock taken) is visible to everyone.",
        "Recover both. 2PC by hand, as an operator must: read the coordinator's log, find no decision, presume abort and ROLLBACK PREPARED on each participant (or finish a logged COMMIT). The saga by a new coordinator process that reads its log and compensates. Then check atomicity and money on every row.");
  }

  @Override
  public List<String> requires() {
    return List.of(
        "failure-postgres (max_prepared_transactions = 16)", "a JVM per coordinator run");
  }

  @Override
  public ObjectNode run(int stage, ObjectNode memo, Trace trace) throws Exception {
    db.requireConfigured();
    var result = json.createObjectNode();
    switch (stage) {
      case 0 -> {
        result.set("cleared", json.valueToTree(clear()));
        TwoPhaseSchema.recreate(db);
        var order = "o-" + shortId();
        var before = rows2pc();
        try (var p = coordinator("2pc", order, ACCOUNT_2PC, SKU_2PC, "none")) {
          int code = p.awaitExit(Duration.ofSeconds(30));
          result.put("order", order);
          result.put("exitCode", code);
          result.set("coordinatorEvents", json.valueToTree(p.events()));
          trace.event("2pc committed", Map.of("order", order));
          check(result, "coordinator exited normally", 0, code);
        }
        result.set("before", json.valueToTree(before));
        result.set("after", json.valueToTree(rows2pc()));
        result.set("log", json.valueToTree(log(null)));
        var prepared = inspector.prepared();
        check(result, "prepared transactions left", 0, prepared.size());
        check(
            result,
            "payment captured and stock taken, together",
            "1 payment + 1 reservation",
            countOrder(order),
            countOrder(order).equals("1 payment + 1 reservation"));
      }
      case 1 -> {
        var order = "o-" + shortId();
        memo.put("order2pc", order);
        try (var p = coordinator("2pc", order, ACCOUNT_2PC, SKU_2PC, "after-prepare")) {
          processes.add(p);
          var paused = p.await("PAUSED", Duration.ofSeconds(30));
          var sessionsBefore = inspector.sessions(List.of(PAYMENTS, INVENTORY, COORDINATOR));
          int code = p.kill();
          processes.remove(p);
          trace.event(
              "coordinator SIGKILL",
              Map.of("pid", String.valueOf(p.pid()), "exit", String.valueOf(code)));
          result.put("order", order);
          result.put("coordinatorPid", p.pid());
          result.put("killedAt", paused.path("at").asString());
          result.put("exitCode", code);
          result.put("signal", code == 137 ? "SIGKILL (128 + 9)" : "exit " + code);
          result.set("coordinatorEvents", json.valueToTree(p.events()));
          result.set("sessionsWhileAlive", json.valueToTree(sessionsBefore));
          check(result, "coordinator killed by SIGKILL", "137", code, code == 137);
        }
        Thread.sleep(300); // the server notices the closed sockets
        var prepared = inspector.prepared();
        result.set("preparedTransactions", json.valueToTree(prepared));
        result.set("coordinatorLog", json.valueToTree(log("zs2pc-" + order)));
        var sessions = inspector.sessions(List.of(PAYMENTS, INVENTORY, COORDINATOR));
        result.set("sessionsAfterKill", json.valueToTree(sessions));
        check(result, "prepared transactions in doubt", 2, prepared.size());
        check(
            result,
            "decision in the coordinator's log",
            "none",
            decision("zs2pc-" + order),
            decision("zs2pc-" + order) == null);
        check(result, "coordinator sessions left", 0, sessions.size());
      }
      case 2 -> {
        var order = memo.path("order2pc").asString();
        var locks = new ArrayList<Map<String, Object>>();
        locks.addAll(inspector.preparedLocks(PAYMENTS));
        locks.addAll(inspector.preparedLocks(INVENTORY));
        result.set("preparedLocks", json.valueToTree(locks));
        check(result, "locks held with no session (pid null)", "at least 4: a table and index lock per participant, and its xid",
            locks.size(), locks.size() >= 4 && locks.stream().allMatch(l -> l.get("pid") == null));
        var stockRow = inspector.tuple(INVENTORY, STOCK_ROW, SKU_2PC);
        var accountRow = inspector.tuple(PAYMENTS, ACCOUNT_ROW, ACCOUNT_2PC);
        result.set("stockRowAsStored", json.valueToTree(stockRow));
        result.set("accountRowAsStored", json.valueToTree(accountRow));
        var prepared = inspector.prepared();
        var inventoryXid =
            prepared.stream()
                .filter(p -> p.get("gid").toString().endsWith("-inventory"))
                .map(p -> p.get("xid"))
                .findFirst()
                .orElse(null);
        check(
            result,
            "stock row's xmax is the prepared transaction",
            String.valueOf(inventoryXid),
            stockRow.get("xmax"),
            String.valueOf(inventoryXid).equals(String.valueOf(stockRow.get("xmax"))));
        var probes = inspector.probe(standardProbes(SKU_2PC, ACCOUNT_2PC), LOCK_TIMEOUT_MS);
        result.set("otherWork", json.valueToTree(probes));
        var sameSku = find(probes, "checkout, same SKU");
        var payment = find(probes, "payment, same account");
        var other = find(probes, "checkout, other SKU");
        trace.event(
            "checkout blocked",
            Map.of(
                "waitedMs",
                String.valueOf(sameSku.elapsedMs()),
                "blockedBy",
                String.valueOf(sameSku.blockedBy())));
        check(
            result,
            "checkout of the same SKU",
            "LOCK_TIMEOUT after " + LOCK_TIMEOUT_MS + " ms",
            sameSku.outcome() + " after " + sameSku.elapsedMs() + " ms",
            sameSku.outcome().equals("LOCK_TIMEOUT")
                && sameSku.elapsedMs() >= LOCK_TIMEOUT_MS - 200);
        check(
            result,
            "it was blocked by pid 0 (a prepared transaction)",
            "[0]",
            sameSku.blockedBy(),
            sameSku.blockedBy().equals(List.of(0)));
        check(result, "payment on the same account", "LOCK_TIMEOUT", payment.outcome());
        check(
            result,
            "checkout of another SKU",
            "DONE within 1 s",
            other.outcome() + " in " + other.elapsedMs() + " ms",
            other.outcome().equals("DONE") && other.elapsedMs() < 1000);
        var visible =
            inspector.tuple(INVENTORY, "SELECT on_hand FROM stock WHERE sku = ?", SKU_2PC);
        result.set("readersSee", json.valueToTree(visible));
        check(
            result,
            "readers are not blocked and see the stock before the in-doubt checkout",
            STOCK - QUANTITY,
            visible.get("on_hand"));
        // Every session the probes and the coordinator had is gone; the prepared work is not.
        var sessions = inspector.sessions(List.of(PAYMENTS, INVENTORY));
        result.set("sessionsOnParticipants", json.valueToTree(sessions));
        check(result, "prepared transactions still in doubt with no client session on either participant", "2 prepared, 0 sessions",
            inspector.prepared().size() + " prepared, " + sessions.size() + " sessions", inspector.prepared().size() == 2 && sessions.isEmpty());
        var vacuum = inspector.vacuum(INVENTORY, 200);
        result.set("vacuum", json.valueToTree(vacuum));
        memo.put("deadNotRemovable", ((Number) vacuum.get("deadNotRemovable")).longValue());
        check(
            result,
            "VACUUM cannot remove rows dead after the prepared transaction began",
            "200 or more",
            vacuum.get("deadNotRemovable"),
            ((Number) vacuum.get("deadNotRemovable")).longValue() >= 200);
        memo.put("blockedMs2pc", sameSku.elapsedMs());
        memo.put("locks2pc", locks.size());
        compare(
            result,
            "Locks held after the coordinator crash",
            locks.size() + " (" + order + ", no session owns them)",
            null);
        compare(
            result,
            "Checkout of the same SKU",
            sameSku.outcome() + " after " + sameSku.elapsedMs() + " ms, blocked by pid 0",
            null);
        compare(
            result, "Half-done state visible to others", "no: prepared work is invisible", null);
      }
      case 3 -> {
        var order = "o-" + shortId();
        memo.put("orderSaga", order);
        try (var p = coordinator("saga", order, ACCOUNT_SAGA, SKU_SAGA, "after-payment")) {
          processes.add(p);
          p.await("PAUSED", Duration.ofSeconds(30));
          int code = p.kill();
          processes.remove(p);
          trace.event(
              "saga coordinator SIGKILL",
              Map.of("pid", String.valueOf(p.pid()), "exit", String.valueOf(code)));
          result.put("order", order);
          result.put("coordinatorPid", p.pid());
          result.put("exitCode", code);
          result.set("coordinatorEvents", json.valueToTree(p.events()));
          check(result, "saga coordinator killed by SIGKILL", "137", code, code == 137);
        }
        Thread.sleep(300);
        var sagaPrepared =
            inspector.prepared().stream()
                .filter(p -> p.get("gid").toString().contains(order))
                .toList();
        check(result, "prepared transactions of the saga", 0, sagaPrepared.size());
        result.set("sagaLog", json.valueToTree(log("zssaga-" + order)));
        check(result, "saga log state", "PAYMENT_DONE", state("zssaga-" + order));
        var probes = inspector.probe(standardProbes(SKU_SAGA, ACCOUNT_SAGA), LOCK_TIMEOUT_MS);
        result.set("otherWork", json.valueToTree(probes));
        var sameSku = find(probes, "checkout, same SKU");
        var payment = find(probes, "payment, same account");
        check(
            result,
            "checkout of the same SKU",
            "DONE within 1 s",
            sameSku.outcome() + " in " + sameSku.elapsedMs() + " ms",
            sameSku.outcome().equals("DONE") && sameSku.elapsedMs() < 1000);
        check(
            result,
            "payment on the same account",
            "DONE within 1 s",
            payment.outcome() + " in " + payment.elapsedMs() + " ms",
            payment.outcome().equals("DONE") && payment.elapsedMs() < 1000);
        var halfDone = halfDone(order);
        result.set("halfDoneOrder", json.valueToTree(halfDone));
        check(
            result,
            "payment captured, stock not taken: visible to everyone",
            "CAPTURED / no reservation",
            halfDone.get("payment") + " / " + halfDone.get("reservation"),
            "CAPTURED".equals(halfDone.get("payment")) && halfDone.get("reservation") == null);
        compare(
            result,
            "Locks held after the coordinator crash",
            null,
            "0 (each step already committed)");
        compare(
            result,
            "Checkout of the same SKU",
            null,
            sameSku.outcome() + " in " + sameSku.elapsedMs() + " ms");
        compare(
            result,
            "Half-done state visible to others",
            null,
            "yes: paid, no stock taken (no isolation between steps)");
      }
      case 4 -> {
        var order2pc = memo.path("order2pc").asString();
        var orderSaga = memo.path("orderSaga").asString();
        long t0 = System.nanoTime();
        var manual = recoverInDoubt();
        long manualMs = (System.nanoTime() - t0) / 1_000_000;
        result.set("twoPhaseRecovery", json.valueToTree(manual));
        trace.event("2pc resolved", Map.of("actions", String.valueOf(manual.size())));
        check(result, "prepared transactions left", 0, inspector.prepared().size());
        var after = inspector.probe(standardProbes(SKU_2PC, ACCOUNT_2PC), LOCK_TIMEOUT_MS);
        result.set("otherWorkAfterRecovery", json.valueToTree(after));
        var sameSku = find(after, "checkout, same SKU");
        check(
            result,
            "checkout of the 2PC SKU after recovery",
            "DONE within 1 s",
            sameSku.outcome() + " in " + sameSku.elapsedMs() + " ms",
            sameSku.outcome().equals("DONE") && sameSku.elapsedMs() < 1000);
        var vacuum = inspector.vacuum(INVENTORY, 0);
        result.set("vacuumAfterRecovery", json.valueToTree(vacuum));
        check(
            result,
            "VACUUM removes what it could not before",
            "0 dead but not removable",
            vacuum.get("deadNotRemovable"),
            ((Number) vacuum.get("deadNotRemovable")).longValue() == 0);

        long s0 = System.nanoTime();
        // The saga's step deadline is 3 s after it started; a recovery only compensates past it.
        waitPastDeadline("zssaga-" + orderSaga);
        try (var p = coordinator("saga-recover", orderSaga, ACCOUNT_SAGA, SKU_SAGA, "none")) {
          int code = p.awaitExit(Duration.ofSeconds(30));
          long sagaMs = (System.nanoTime() - s0) / 1_000_000;
          result.set("sagaRecovery", json.valueToTree(p.events()));
          result.put("sagaRecoveryExitCode", code);
          result.put("sagaRecoveryMs", sagaMs);
          check(result, "recovery coordinator exited normally", 0, code);
          compare(
              result,
              "Recovery",
              "by hand: read the log, "
                  + manual.size()
                  + " × ROLLBACK/COMMIT PREPARED ("
                  + manualMs
                  + " ms once someone acts)",
              "automatic: a new coordinator compensated from its log ("
                  + sagaMs
                  + " ms, most of it waiting for the step deadline)");
        }
        trace.event("saga compensated", Map.of("order", orderSaga));
        check(result, "saga state after recovery", "COMPENSATED", state("zssaga-" + orderSaga));
        var atomic = countOrder(order2pc);
        result.put("twoPhaseOrderOutcome", atomic);
        check(
            result,
            "2PC order is all or nothing on both participants",
            "0 + 0 or 1 + 1",
            atomic,
            atomic.equals("0 payment + 0 reservation")
                || atomic.equals("1 payment + 1 reservation"));
        var money = money();
        result.set("invariants", json.valueToTree(money));
        check(
            result,
            "every balance = opening balance − captured payments",
            true,
            money.get("balancesMatchPayments"));
        check(
            result,
            "every stock level = opening stock − reservations",
            true,
            money.get("stockMatchesReservations"));
        var saga = halfDone(orderSaga);
        result.set("sagaOrder", json.valueToTree(saga));
        check(
            result,
            "saga payment refunded, no stock taken",
            "REFUNDED / no reservation",
            saga.get("payment") + " / " + saga.get("reservation"),
            "REFUNDED".equals(saga.get("payment")) && saga.get("reservation") == null);
        compare(
            result,
            "Final state",
            "consistent after the operator acted (" + atomic + ")",
            "consistent: payment refunded, nothing reserved");
      }
      default -> throw new IllegalArgumentException("No stage " + stage);
    }
    return result;
  }

  // ---- manual recovery (also the inspector's buttons)
  // ---------------------------------------------

  /**
   * The operator's procedure: for each in-doubt 2PC branch, read the coordinator's log. A durable
   * COMMIT decision must be finished (COMMIT PREPARED); no decision means the coordinator never
   * decided, so presumed abort (ROLLBACK PREPARED) is safe.
   */
  public List<Map<String, Object>> recoverInDoubt() throws SQLException {
    var actions = new ArrayList<Map<String, Object>>();
    for (var p : inspector.prepared()) {
      String gid = p.get("gid").toString();
      if (!gid.startsWith("zs2pc-")) continue;
      String global = gid.replaceFirst("-(payments|inventory)$", "");
      String decision = decision(global);
      String action = "COMMIT".equals(decision) ? "COMMIT" : "ROLLBACK";
      resolve(gid, action, "recovery");
      var row = new LinkedHashMap<String, Object>();
      row.put("gid", gid);
      row.put("database", p.get("database"));
      row.put("loggedDecision", decision == null ? "none" : decision);
      row.put("action", action + " PREPARED");
      row.put(
          "why",
          decision == null
              ? "no decision was logged: presumed abort"
              : "the decision was durable: it must be finished");
      actions.add(row);
    }
    return actions;
  }

  /** COMMIT PREPARED or ROLLBACK PREPARED one branch, recorded in the coordinator's log. */
  public Map<String, Object> resolve(String gid, String action, String actor) throws SQLException {
    if (!gid.matches("zs2pc-[a-z0-9-]+-(payments|inventory)"))
      throw new IllegalArgumentException("Not one of this lab's prepared transactions: " + gid);
    if (!action.equals("COMMIT") && !action.equals("ROLLBACK"))
      throw new IllegalArgumentException("COMMIT or ROLLBACK, not " + action);
    var database = gid.endsWith("-payments") ? PAYMENTS : INVENTORY;
    try (var c = db.connect(database);
        var s = c.createStatement()) {
      s.execute(action + " PREPARED '" + gid + "'");
    }
    String global = gid.replaceFirst("-(payments|inventory)$", "");
    try (var c = db.connect(COORDINATOR);
        var s =
            c.prepareStatement(
                "INSERT INTO coordinator_event(gid, actor, action, detail) VALUES (?, ?, ?, ?)")) {
      s.setString(1, global);
      s.setString(2, actor);
      s.setString(3, action + "_PREPARED");
      s.setString(4, gid);
      s.executeUpdate();
    }
    return Map.of("gid", gid, "action", action + " PREPARED", "database", database);
  }

  /** Starts a 2PC checkout that the coordinator abandons at {@code crashAt}, for manual drills. */
  public Map<String, Object> startInDoubt(String crashAt) throws Exception {
    if (!crashAt.equals("after-prepare") && !crashAt.equals("after-decision"))
      throw new IllegalArgumentException("crashAt is after-prepare or after-decision");
    if (!db.exists(COORDINATOR)) TwoPhaseSchema.recreate(db);
    var order = "o-" + shortId();
    try (var p = coordinator("2pc", order, ACCOUNT_2PC, SKU_2PC, crashAt)) {
      p.await("PAUSED", Duration.ofSeconds(30));
      int code = p.kill();
      var out = new LinkedHashMap<String, Object>();
      out.put("order", order);
      out.put("crashAt", crashAt);
      out.put("coordinatorPid", p.pid());
      out.put("exitCode", code);
      out.put("decision", decision("zs2pc-" + order));
      return out;
    }
  }

  // ---- inspector and reset ----------------------------------------------------------------------

  @Override
  public ObjectNode state() throws Exception {
    var state = json.createObjectNode();
    state.set(
        "processes",
        json.valueToTree(
            processes.stream()
                .map(
                    p ->
                        Map.of(
                            "pid",
                            p.pid(),
                            "mode",
                            p.mode(),
                            "alive",
                            p.alive(),
                            "started",
                            p.started().toString()))
                .toList()));
    if (!db.exists(COORDINATOR)) {
      state.put("ready", false);
      state.put("note", "Not set up yet: run the first stage.");
      state.set("preparedTransactions", json.valueToTree(inspector.prepared()));
      return state;
    }
    state.put("ready", true);
    var prepared = inspector.prepared();
    var withDecision = new ArrayList<Map<String, Object>>();
    for (var p : prepared) {
      var row = new LinkedHashMap<>(p);
      var gid = p.get("gid").toString();
      row.put(
          "loggedDecision",
          gid.startsWith("zs2pc-")
              ? String.valueOf(decision(gid.replaceFirst("-(payments|inventory)$", "")))
              : "not this lab's");
      withDecision.add(row);
    }
    state.set("preparedTransactions", json.valueToTree(withDecision));
    var locks = new ArrayList<Map<String, Object>>();
    locks.addAll(inspector.preparedLocks(PAYMENTS));
    locks.addAll(inspector.preparedLocks(INVENTORY));
    state.set("preparedLocks", json.valueToTree(locks));
    state.set("waiting", json.valueToTree(inspector.waiting()));
    state.set("coordinatorLog", json.valueToTree(log(null)));
    try (var c = db.connect(COORDINATOR)) {
      state.set(
          "coordinatorEvents",
          json.valueToTree(
              FailureDb.rows(
                  c,
                  "SELECT at, gid, actor, action, detail FROM coordinator_event ORDER BY id DESC LIMIT 20")));
    }
    state.set("rows", json.valueToTree(rows2pc()));
    return state;
  }

  @Override
  public ObjectNode reset() throws Exception {
    db.requireConfigured();
    var result = json.createObjectNode();
    for (var p : processes) p.close();
    processes.clear();
    result.set("resolved", json.valueToTree(clear()));
    TwoPhaseSchema.recreate(db);
    result.put("recreated", String.join(", ", DATABASES));
    return result;
  }

  /**
   * Rolls back every prepared transaction of this lab (a database holding one cannot be dropped).
   */
  private List<String> clear() throws SQLException {
    var done = new ArrayList<String>();
    for (var p : inspector.prepared()) {
      var gid = p.get("gid").toString();
      if (!gid.startsWith("zs2pc-")) continue;
      try (var c = db.connect(p.get("database").toString());
          var s = c.createStatement()) {
        s.execute("ROLLBACK PREPARED '" + gid + "'");
        done.add(gid);
      }
    }
    return done;
  }

  // ---- helpers
  // ------------------------------------------------------------------------------------

  private CoordinatorProcess coordinator(
      String mode, String order, String account, String sku, String pause) throws Exception {
    var args = new LinkedHashMap<String, String>();
    args.put("payments", db.url(PAYMENTS));
    args.put("inventory", db.url(INVENTORY));
    args.put("coordinator", db.url(COORDINATOR));
    args.put("user", db.user());
    args.put("order", order);
    args.put("account", account);
    args.put("amount", AMOUNT.toPlainString());
    args.put("sku", sku);
    args.put("quantity", String.valueOf(QUANTITY));
    args.put("pause", pause);
    return CoordinatorProcess.start(mode, args, db.password());
  }

  private static List<PostgresInspector.Probe> standardProbes(String sku, String account) {
    return List.of(
        new PostgresInspector.Probe(
            "checkout, same SKU",
            INVENTORY,
            "UPDATE stock SET on_hand = on_hand - 1 WHERE sku = ?",
            sku),
        new PostgresInspector.Probe(
            "payment, same account",
            PAYMENTS,
            "UPDATE account SET balance = balance - 5 WHERE id = ?",
            account),
        new PostgresInspector.Probe(
            "checkout, other SKU",
            INVENTORY,
            "UPDATE stock SET on_hand = on_hand - 1 WHERE sku = ?",
            SKU_OTHER));
  }

  private static PostgresInspector.ProbeResult find(
      List<PostgresInspector.ProbeResult> all, String name) {
    return all.stream().filter(p -> p.name().equals(name)).findFirst().orElseThrow();
  }

  private List<Map<String, Object>> log(String gid) throws SQLException {
    try (var c = db.connect(COORDINATOR)) {
      return gid == null
          ? FailureDb.rows(
              c,
              "SELECT gid, protocol, order_id, state, decision, deadline, started_at, updated_at FROM coordinator_log ORDER BY started_at DESC LIMIT 20")
          : FailureDb.rows(
              c,
              "SELECT l.gid, l.protocol, l.state, l.decision, e.at, e.actor, e.action, e.detail FROM coordinator_log l"
                  + " JOIN coordinator_event e ON e.gid = l.gid WHERE l.gid = ? ORDER BY e.id",
              gid);
    }
  }

  private String decision(String gid) throws SQLException {
    try (var c = db.connect(COORDINATOR)) {
      return (String)
          FailureDb.one(c, "SELECT decision FROM coordinator_log WHERE gid = ?", gid)
              .get("decision");
    }
  }

  private String state(String gid) throws SQLException {
    try (var c = db.connect(COORDINATOR)) {
      return (String)
          FailureDb.one(c, "SELECT state FROM coordinator_log WHERE gid = ?", gid).get("state");
    }
  }

  private void waitPastDeadline(String gid) throws Exception {
    try (var c = db.connect(COORDINATOR)) {
      var left =
          FailureDb.one(
              c,
              "SELECT greatest(0, extract(epoch FROM deadline - now()) * 1000)::bigint AS ms FROM coordinator_log WHERE gid = ?",
              gid);
      long ms = left.isEmpty() ? 0 : ((Number) left.get("ms")).longValue();
      if (ms > 0) Thread.sleep(ms + 100);
    }
  }

  private Map<String, Object> rows2pc() throws SQLException {
    var rows = new LinkedHashMap<String, Object>();
    try (var c = db.connect(PAYMENTS)) {
      rows.put("accounts", FailureDb.rows(c, "SELECT id, balance FROM account ORDER BY id"));
      rows.put(
          "payments",
          FailureDb.rows(
              c, "SELECT order_id, account_id, amount, status FROM payment ORDER BY at"));
    }
    try (var c = db.connect(INVENTORY)) {
      rows.put("stock", FailureDb.rows(c, "SELECT sku, on_hand FROM stock ORDER BY sku"));
      rows.put(
          "reservations",
          FailureDb.rows(c, "SELECT order_id, sku, quantity, status FROM reservation ORDER BY at"));
    }
    return rows;
  }

  private String countOrder(String order) throws SQLException {
    long payments;
    long reservations;
    try (var c = db.connect(PAYMENTS)) {
      payments =
          ((Number)
                  FailureDb.one(
                          c,
                          "SELECT count(*) AS n FROM payment WHERE order_id = ? AND status = 'CAPTURED'",
                          order)
                      .get("n"))
              .longValue();
    }
    try (var c = db.connect(INVENTORY)) {
      reservations =
          ((Number)
                  FailureDb.one(
                          c, "SELECT count(*) AS n FROM reservation WHERE order_id = ?", order)
                      .get("n"))
              .longValue();
    }
    return payments + " payment + " + reservations + " reservation";
  }

  private Map<String, Object> halfDone(String order) throws SQLException {
    var out = new LinkedHashMap<String, Object>();
    out.put("order", order);
    try (var c = db.connect(PAYMENTS)) {
      out.put(
          "payment",
          FailureDb.one(c, "SELECT status FROM payment WHERE order_id = ?", order).get("status"));
      out.put(
          "balance",
          FailureDb.one(c, "SELECT balance FROM account WHERE id = ?", ACCOUNT_SAGA)
              .get("balance"));
    }
    try (var c = db.connect(INVENTORY)) {
      out.put(
          "reservation",
          FailureDb.one(c, "SELECT status FROM reservation WHERE order_id = ?", order)
              .get("status"));
      out.put(
          "onHand",
          FailureDb.one(c, "SELECT on_hand FROM stock WHERE sku = ?", SKU_SAGA).get("on_hand"));
    }
    return out;
  }

  /** Money and stock reconciled row by row against the payments and reservations that exist. */
  private Map<String, Object> money() throws SQLException {
    var out = new LinkedHashMap<String, Object>();
    try (var c = db.connect(PAYMENTS)) {
      var rows =
          FailureDb.rows(
              c,
              "SELECT a.id, a.balance, ?::numeric - coalesce((SELECT sum(amount) FROM payment p"
                  + " WHERE p.account_id = a.id AND p.status = 'CAPTURED'), 0) AS expected FROM account a ORDER BY a.id",
              BALANCE);
      out.put("accounts", rows);
      out.put(
          "balancesMatchPayments",
          rows.stream()
              .allMatch(
                  r ->
                      ((BigDecimal) r.get("balance")).compareTo((BigDecimal) r.get("expected"))
                          == 0));
    }
    try (var c = db.connect(INVENTORY)) {
      var rows =
          FailureDb.rows(
              c,
              "SELECT s.sku, s.on_hand, ? - coalesce((SELECT sum(quantity) FROM reservation r"
                  + " WHERE r.sku = s.sku), 0) AS expected FROM stock s ORDER BY s.sku",
              STOCK);
      out.put("stock", rows);
      out.put(
          "stockMatchesReservations",
          rows.stream()
              .allMatch(
                  r ->
                      ((Number) r.get("on_hand")).longValue()
                          == ((Number) r.get("expected")).longValue()));
    }
    return out;
  }

  private static String shortId() {
    return java.util.UUID.randomUUID().toString().substring(0, 8);
  }
}
