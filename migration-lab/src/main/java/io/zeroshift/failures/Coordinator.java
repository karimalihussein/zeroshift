package io.zeroshift.failures;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The transaction coordinator, run as its own JVM process by the control plane so that crashing it
 * is a real SIGKILL: its memory, its connections and every decision it had not written down are
 * gone. It speaks JSON lines on stdout. At a pause point it reports PAUSED and waits for a line on
 * stdin; the lab kills it there, which makes the crash land at the same protocol step every time.
 *
 * <p>Modes: {@code 2pc} (PREPARE TRANSACTION on both participants, decision, COMMIT PREPARED),
 * {@code saga} (local commit per step, durable saga log) and {@code saga-recover} (a fresh
 * coordinator finishing interrupted sagas from the log by compensation).
 *
 * <p>Arguments are key=value pairs: mode, payments, inventory, coordinator (JDBC URLs), user,
 * order, account, amount, sku, quantity, pause (after-prepare, after-decision, after-payment,
 * none). The password comes from the environment (FL_DB_PASSWORD), never the command line.
 */
public final class Coordinator {
  private final Map<String, String> args;
  private final String password = System.getenv("FL_DB_PASSWORD");
  private final BufferedReader stdin =
      new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));

  private Coordinator(Map<String, String> args) {
    this.args = args;
  }

  public static void main(String[] argv) throws Exception {
    var args = new LinkedHashMap<String, String>();
    for (var a : argv) {
      int eq = a.indexOf('=');
      args.put(a.substring(0, eq), a.substring(eq + 1));
    }
    var coordinator = new Coordinator(args);
    try {
      switch (args.get("mode")) {
        case "2pc" -> coordinator.twoPhase();
        case "saga" -> coordinator.saga();
        case "saga-recover" -> coordinator.recoverSagas();
        default -> throw new IllegalArgumentException("mode " + args.get("mode"));
      }
      emit("EXITED", "code", "0");
      System.exit(0);
    } catch (Exception e) {
      emit("ERROR", "error", e.getClass().getSimpleName() + ": " + e.getMessage());
      System.exit(2);
    }
  }

  // ---- two-phase commit ----------------------------------------------------------------------

  private void twoPhase() throws Exception {
    String order = args.get("order");
    String gid = "zs2pc-" + order;
    try (var log = connect("coordinator");
        var payments = connect("payments");
        var inventory = connect("inventory")) {
      write(
          log,
          "INSERT INTO coordinator_log(gid, protocol, order_id, state) VALUES (?, '2PC', ?, 'STARTED')",
          gid,
          order);
      event(log, gid, "coordinator", "BEGIN", "2PC for " + order);
      emit("STARTED", "gid", gid, "pid", pid());

      // Phase 1: each participant does its work and votes by preparing. After PREPARE the work is
      // on disk, holds its locks, and belongs to no session: only COMMIT/ROLLBACK PREPARED end it.
      prepare(
          payments,
          gid + "-payments",
          "payments",
          () -> {
            debit(payments, args.get("account"), amount());
            write(
                payments,
                "INSERT INTO payment(order_id, account_id, amount, status) VALUES (?, ?, ?, 'CAPTURED')",
                order,
                args.get("account"),
                amount());
          });
      event(log, gid, "payments", "PREPARED", gid + "-payments");
      prepare(
          inventory,
          gid + "-inventory",
          "inventory",
          () -> {
            take(inventory, args.get("sku"), quantity());
            write(
                inventory,
                "INSERT INTO reservation(order_id, sku, quantity, status) VALUES (?, ?, ?, 'RESERVED')",
                order,
                args.get("sku"),
                quantity());
          });
      event(log, gid, "inventory", "PREPARED", gid + "-inventory");
      write(
          log,
          "UPDATE coordinator_log SET state = 'PREPARED', updated_at = now() WHERE gid = ?",
          gid);
      emit("PREPARED", "gid", gid);
      pause("after-prepare");

      // The decision is the commit point: once it is durable, the transaction must commit on
      // every participant, whatever crashes next.
      write(
          log,
          "UPDATE coordinator_log SET state = 'DECIDED', decision = 'COMMIT', updated_at = now() WHERE gid = ?",
          gid);
      event(log, gid, "coordinator", "DECIDED", "COMMIT");
      emit("DECIDED", "decision", "COMMIT");
      pause("after-decision");

      for (var p : new String[] {"payments", "inventory"}) {
        var c = p.equals("payments") ? payments : inventory;
        try (var s = c.createStatement()) {
          s.execute("COMMIT PREPARED '" + gid + "-" + p + "'");
        }
        event(log, gid, p, "COMMITTED", gid + "-" + p);
        emit("COMMITTED", "participant", p);
      }
      write(
          log,
          "UPDATE coordinator_log SET state = 'COMMITTED', updated_at = now() WHERE gid = ?",
          gid);
    }
  }

  private void prepare(Connection c, String gid, String participant, SqlWork work)
      throws Exception {
    c.setAutoCommit(false);
    String txid;
    String backend;
    try (var s = c.createStatement();
        var rs = s.executeQuery("SELECT pg_current_xact_id()::text, pg_backend_pid()")) {
      rs.next();
      txid = rs.getString(1);
      backend = rs.getString(2);
    }
    work.run();
    try (var s = c.createStatement()) {
      s.execute("PREPARE TRANSACTION '" + gid + "'");
    }
    c.setAutoCommit(true);
    emit("VOTED_YES", "participant", participant, "gid", gid, "txid", txid, "backendPid", backend);
  }

  // ---- saga ------------------------------------------------------------------------------------

  private void saga() throws Exception {
    String order = args.get("order");
    String gid = "zssaga-" + order;
    try (var log = connect("coordinator");
        var payments = connect("payments");
        var inventory = connect("inventory")) {
      write(
          log,
          "INSERT INTO coordinator_log(gid, protocol, order_id, state, deadline)"
              + " VALUES (?, 'SAGA', ?, 'STARTED', now() + interval '3 seconds')",
          gid,
          order);
      event(log, gid, "coordinator", "BEGIN", "saga for " + order);
      emit("STARTED", "gid", gid, "pid", pid());

      // Step 1, a local transaction: committed and visible at once, holding no lock afterwards.
      local(
          payments,
          () -> {
            debit(payments, args.get("account"), amount());
            write(
                payments,
                "INSERT INTO payment(order_id, account_id, amount, status) VALUES (?, ?, ?, 'CAPTURED')",
                order,
                args.get("account"),
                amount());
          });
      write(
          log,
          "UPDATE coordinator_log SET state = 'PAYMENT_DONE', updated_at = now() WHERE gid = ?",
          gid);
      event(log, gid, "payments", "COMMITTED", "payment captured (local transaction)");
      emit("STEP_DONE", "step", "payment");
      pause("after-payment");

      local(
          inventory,
          () -> {
            take(inventory, args.get("sku"), quantity());
            write(
                inventory,
                "INSERT INTO reservation(order_id, sku, quantity, status) VALUES (?, ?, ?, 'RESERVED')",
                order,
                args.get("sku"),
                quantity());
          });
      write(
          log,
          "UPDATE coordinator_log SET state = 'COMPLETED', updated_at = now() WHERE gid = ?",
          gid);
      event(log, gid, "inventory", "COMMITTED", "stock reserved (local transaction)");
      emit("STEP_DONE", "step", "inventory");
    }
  }

  /**
   * A new coordinator after a crash: every saga that is neither completed nor compensated and is
   * past its deadline is compensated. Each compensation is idempotent (it only refunds a payment
   * still CAPTURED), so a recovery that crashes and runs again refunds once.
   */
  private void recoverSagas() throws Exception {
    try (var log = connect("coordinator");
        var payments = connect("payments")) {
      emit("STARTED", "pid", pid());
      var open =
          FailureDb.rows(
              log,
              "SELECT gid, order_id, state FROM coordinator_log WHERE protocol = 'SAGA'"
                  + " AND state NOT IN ('COMPLETED', 'COMPENSATED', 'ABORTED') AND deadline < now() ORDER BY started_at");
      for (var saga : open) {
        String gid = (String) saga.get("gid");
        String order = (String) saga.get("order_id");
        event(log, gid, "recovery", "FOUND", "state " + saga.get("state") + ", past its deadline");
        emit("FOUND", "gid", gid, "state", (String) saga.get("state"));
        var refunded = new BigDecimal[1];
        local(
            payments,
            () -> {
              var row =
                  FailureDb.one(
                      payments,
                      "UPDATE payment SET status = 'REFUNDED' WHERE order_id = ? AND status = 'CAPTURED'"
                          + " RETURNING account_id, amount",
                      order);
              if (!row.isEmpty()) {
                refunded[0] = (BigDecimal) row.get("amount");
                write(
                    payments,
                    "UPDATE account SET balance = balance + ? WHERE id = ?",
                    refunded[0],
                    row.get("account_id"));
              }
            });
        String outcome = refunded[0] == null ? "ABORTED" : "COMPENSATED";
        write(
            log,
            "UPDATE coordinator_log SET state = ?, updated_at = now() WHERE gid = ?",
            outcome,
            gid);
        event(
            log,
            gid,
            "payments",
            refunded[0] == null ? "NOTHING_TO_UNDO" : "REFUNDED",
            refunded[0] == null ? "no captured payment" : "refunded " + refunded[0]);
        emit(
            outcome,
            "gid",
            gid,
            "refunded",
            refunded[0] == null ? "0" : refunded[0].toPlainString());
      }
      emit("RECOVERED", "sagas", String.valueOf(open.size()));
    }
  }

  // ---- participants' work
  // ------------------------------------------------------------------------

  private static void debit(Connection c, String account, BigDecimal amount) throws SQLException {
    try (var s =
        c.prepareStatement(
            "UPDATE account SET balance = balance - ? WHERE id = ? AND balance >= ?")) {
      s.setBigDecimal(1, amount);
      s.setString(2, account);
      s.setBigDecimal(3, amount);
      if (s.executeUpdate() != 1) throw new SQLException("insufficient funds on " + account);
    }
  }

  private static void take(Connection c, String sku, int quantity) throws SQLException {
    try (var s =
        c.prepareStatement(
            "UPDATE stock SET on_hand = on_hand - ? WHERE sku = ? AND on_hand >= ?")) {
      s.setInt(1, quantity);
      s.setString(2, sku);
      s.setInt(3, quantity);
      if (s.executeUpdate() != 1) throw new SQLException("out of stock: " + sku);
    }
  }

  private static void local(Connection c, SqlWork work) throws Exception {
    c.setAutoCommit(false);
    try {
      work.run();
      c.commit();
    } catch (Exception e) {
      c.rollback();
      throw e;
    } finally {
      c.setAutoCommit(true);
    }
  }

  private static void write(Connection c, String sql, Object... values) throws SQLException {
    try (var s = c.prepareStatement(sql)) {
      for (int i = 0; i < values.length; i++) s.setObject(i + 1, values[i]);
      s.executeUpdate();
    }
  }

  private static void event(Connection log, String gid, String actor, String action, String detail)
      throws SQLException {
    write(
        log,
        "INSERT INTO coordinator_event(gid, actor, action, detail) VALUES (?, ?, ?, ?)",
        gid,
        actor,
        action,
        detail);
  }

  private void pause(String point) throws Exception {
    if (!point.equals(args.getOrDefault("pause", "none"))) return;
    emit("PAUSED", "at", point, "pid", pid());
    // Blocks until the control plane writes a line (continue) or kills this process.
    stdin.readLine();
    emit("RESUMED", "at", point);
  }

  private Connection connect(String which) throws SQLException {
    var c = DriverManager.getConnection(args.get(which), args.get("user"), password);
    c.setAutoCommit(true);
    return c;
  }

  private BigDecimal amount() {
    return new BigDecimal(args.get("amount"));
  }

  private int quantity() {
    return Integer.parseInt(args.get("quantity"));
  }

  private static String pid() {
    return String.valueOf(ProcessHandle.current().pid());
  }

  /** One JSON object per line: {"event": …, "at": …, key: value…}. Values are strings. */
  static synchronized void emit(String event, String... pairs) {
    var line =
        new StringBuilder("{\"event\":\"")
            .append(event)
            .append("\",\"at\":\"")
            .append(Instant.now())
            .append('"');
    for (int i = 0; i + 1 < pairs.length; i += 2)
      line.append(",\"").append(pairs[i]).append("\":\"").append(escape(pairs[i + 1])).append('"');
    System.out.println(line.append('}'));
    System.out.flush();
  }

  private static String escape(String s) {
    if (s == null) return "";
    var out = new StringBuilder();
    for (char ch : s.toCharArray()) {
      switch (ch) {
        case '"' -> out.append("\\\"");
        case '\\' -> out.append("\\\\");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> {
          if (ch < 0x20) out.append(String.format("\\u%04x", (int) ch));
          else out.append(ch);
        }
      }
    }
    return out.toString();
  }

  @FunctionalInterface
  interface SqlWork {
    void run() throws Exception;
  }
}
