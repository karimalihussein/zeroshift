package io.zeroshift.failures;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Reads what PostgreSQL itself reports about transactions: pg_prepared_xacts, pg_locks,
 * pg_stat_activity and pg_blocking_pids, the tuple header (xmin, xmax) and VACUUM VERBOSE. And runs
 * "other work" as real sessions to measure whether, and on whom, it waits.
 */
final class PostgresInspector {
  private static final Pattern DEAD = Pattern.compile("(\\d+) are dead but not yet removable");
  private static final Pattern REMOVED = Pattern.compile("tuples: (\\d+) removed");
  private static final Pattern CUTOFF = Pattern.compile("removable cutoff: (\\d+)");

  private final FailureDb db;

  PostgresInspector(FailureDb db) {
    this.db = db;
  }

  /** Every prepared transaction on the server: what an operator sees in doubt. */
  List<Map<String, Object>> prepared() throws SQLException {
    try (var c = db.connect(null)) {
      return FailureDb.rows(
          c,
          "SELECT gid, database, transaction::text AS xid, prepared, age(transaction) AS xid_age,"
              + " round(extract(epoch FROM now() - prepared) * 1000)::bigint AS in_doubt_ms"
              + " FROM pg_prepared_xacts ORDER BY prepared");
    }
  }

  /**
   * Locks held by prepared transactions of one database. Their pid is null: no session owns them.
   * Each is found through the transactionid lock on the prepared transaction's own xid, which
   * names its (dummy) virtual transaction. Row locks are not listed; they live in the rows' xmax,
   * and waiters queue on that transactionid lock.
   */
  List<Map<String, Object>> preparedLocks(String database) throws SQLException {
    try (var c = db.connect(database)) {
      return FailureDb.rows(
          c,
          "SELECT p.gid, l.locktype, CASE WHEN l.relation IS NOT NULL AND l.database ="
              + " (SELECT oid FROM pg_database WHERE datname = current_database())"
              + " THEN l.relation::regclass::text END AS relation, l.mode, l.transactionid::text AS xid,"
              + " l.virtualtransaction, l.pid, l.granted FROM pg_prepared_xacts p"
              + " JOIN pg_locks t ON t.locktype = 'transactionid' AND t.transactionid = p.transaction AND t.pid IS NULL"
              + " JOIN pg_locks l ON l.virtualtransaction = t.virtualtransaction AND l.pid IS NULL"
              + " WHERE p.database = current_database() ORDER BY p.gid, l.locktype, relation");
    }
  }

  /** Sessions waiting for a lock right now, and who blocks them (0 = a prepared transaction). */
  List<Map<String, Object>> waiting() throws SQLException {
    try (var c = db.connect(null)) {
      return FailureDb.rows(
          c,
          "SELECT a.pid, a.datname, a.wait_event_type, a.wait_event, pg_blocking_pids(a.pid) AS blocked_by,"
              + " round(extract(epoch FROM now() - a.query_start) * 1000)::bigint AS waiting_ms,"
              + " left(a.query, 140) AS query FROM pg_stat_activity a WHERE a.wait_event_type = 'Lock'"
              + " ORDER BY a.query_start");
    }
  }

  /** Client sessions connected to the lab's databases, apart from this one. */
  List<Map<String, Object>> sessions(List<String> databases) throws SQLException {
    try (var c = db.connect(null)) {
      return FailureDb.rows(
          c,
          "SELECT pid, datname, application_name, state, backend_start FROM pg_stat_activity"
              + " WHERE datname = ANY(?) AND pid <> pg_backend_pid() AND backend_type = 'client backend'"
              + " ORDER BY backend_start",
          c.createArrayOf("text", databases.toArray()));
    }
  }

  /** The row as stored: xmax names the transaction holding its row lock (or that deleted it). */
  Map<String, Object> tuple(String database, String sql, Object key) throws SQLException {
    try (var c = db.connect(database)) {
      return FailureDb.one(c, sql, key);
    }
  }

  /**
   * Rewrites vacuum_probe {@code updates} times (each a committed transaction leaving one dead row
   * version), then VACUUM VERBOSE: while a transaction older than those rewrites is still in
   * progress (or prepared), PostgreSQL may not remove them.
   */
  Map<String, Object> vacuum(String database, int updates) throws SQLException {
    try (var c = db.connect(database)) {
      try (var s = c.prepareStatement("UPDATE vacuum_probe SET n = n + 1 WHERE id = 1")) {
        for (int i = 0; i < updates; i++) s.executeUpdate();
      }
      var text = new StringBuilder();
      try (var s = c.createStatement()) {
        s.execute("VACUUM (VERBOSE) vacuum_probe");
        for (SQLWarning w = s.getWarnings(); w != null; w = w.getNextWarning())
          text.append(w.getMessage()).append('\n');
      }
      var out = text.toString();
      return Map.of(
          "rewrites", updates,
          "removed", number(REMOVED, out),
          "deadNotRemovable", number(DEAD, out),
          "removableCutoffXid", number(CUTOFF, out),
          "vacuumVerbose",
              out.lines()
                  .filter(l -> l.startsWith("tuples") || l.startsWith("removable"))
                  .toList());
    }
  }

  private static long number(Pattern p, String text) {
    var m = p.matcher(text);
    return m.find() ? Long.parseLong(m.group(1)) : -1;
  }

  // ---- other work -------------------------------------------------------------------------------

  /**
   * Another transaction trying to do its job; it always rolls back so the lab's numbers stay exact.
   */
  record Probe(String name, String database, String sql, Object key) {}

  record ProbeResult(
      String name,
      String database,
      String sql,
      String outcome,
      String sqlstate,
      long elapsedMs,
      boolean waited,
      List<Integer> blockedBy,
      String waitEvent,
      int backendPid,
      String detail) {}

  /**
   * Runs the probes at once, each on its own session with a lock timeout, while a monitor asks
   * pg_stat_activity every 25 ms whether each one is waiting and for whom.
   */
  List<ProbeResult> probe(List<Probe> probes, int lockTimeoutMs) throws Exception {
    var pids = new ConcurrentHashMap<String, Integer>();
    var waits = new ConcurrentHashMap<Integer, Object[]>();
    var done = new AtomicBoolean();
    var ready = new CountDownLatch(probes.size());
    var results = new ArrayList<ProbeResult>();
    try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
      var monitor =
          pool.submit(
              () -> {
                try (var c = db.connect(null);
                    var s =
                        c.prepareStatement(
                            "SELECT pid, wait_event_type, wait_event, pg_blocking_pids(pid) FROM pg_stat_activity"
                                + " WHERE pid = ANY(?) AND wait_event_type = 'Lock'")) {
                  while (!done.get()) {
                    if (!pids.isEmpty()) {
                      s.setArray(1, c.createArrayOf("int4", pids.values().toArray()));
                      try (var rs = s.executeQuery()) {
                        while (rs.next())
                          waits.putIfAbsent(
                              rs.getInt(1),
                              new Object[] {
                                rs.getString(2) + "/" + rs.getString(3),
                                List.of((Integer[]) rs.getArray(4).getArray())
                              });
                      }
                    }
                    Thread.sleep(25);
                  }
                }
                return null;
              });
      var futures = new ArrayList<java.util.concurrent.Future<ProbeResult>>();
      for (var probe : probes)
        futures.add(
            pool.submit(
                () -> {
                  try (var c = db.connect(probe.database())) {
                    c.setAutoCommit(false);
                    int pid;
                    try (var s = c.createStatement()) {
                      s.execute("SET LOCAL lock_timeout = '" + lockTimeoutMs + "ms'");
                      try (var rs = s.executeQuery("SELECT pg_backend_pid()")) {
                        rs.next();
                        pid = rs.getInt(1);
                      }
                    }
                    pids.put(probe.name(), pid);
                    ready.countDown();
                    ready.await(5, TimeUnit.SECONDS);
                    Thread.sleep(60); // the monitor is polling these pids now
                    long t0 = System.nanoTime();
                    String outcome;
                    String state = null;
                    String detail;
                    try (var s = c.prepareStatement(probe.sql())) {
                      s.setObject(1, probe.key());
                      int rows = s.executeUpdate();
                      outcome = "DONE";
                      detail = rows + " row(s) updated, then rolled back";
                    } catch (SQLException e) {
                      outcome = "55P03".equals(e.getSQLState()) ? "LOCK_TIMEOUT" : "ERROR";
                      state = e.getSQLState();
                      detail = e.getMessage().lines().findFirst().orElse("");
                    }
                    long ms = (System.nanoTime() - t0) / 1_000_000;
                    c.rollback();
                    Thread.sleep(30);
                    var wait = waits.get(pid);
                    return new ProbeResult(
                        probe.name(),
                        probe.database(),
                        probe.sql(),
                        outcome,
                        state,
                        ms,
                        wait != null,
                        wait == null ? List.of() : castList(wait[1]),
                        wait == null ? null : (String) wait[0],
                        pid,
                        detail);
                  }
                }));
      for (var f : futures) results.add(f.get(lockTimeoutMs + 15_000L, TimeUnit.MILLISECONDS));
      done.set(true);
      monitor.get(5, TimeUnit.SECONDS);
    }
    return results;
  }

  @SuppressWarnings("unchecked")
  private static List<Integer> castList(Object o) {
    return (List<Integer>) o;
  }
}
