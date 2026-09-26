package io.zeroshift.failures;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

/**
 * Connections to the failure lab's own PostgreSQL server. No pool: the labs drop, copy and restore
 * whole databases, and a pooled connection would keep a dropped database busy. Every database the
 * labs touch is named fl_*, so nothing here can reach another lab's data.
 */
@Component
public class FailureDb {
  private static final Pattern NAME = Pattern.compile("fl_[a-z_]{1,40}");

  private final FailureLabSettings settings;

  public FailureDb(FailureLabSettings settings) {
    this.settings = settings;
  }

  /** Fails with the way to start the lab's infrastructure when it is not configured. */
  public void requireConfigured() {
    if (!settings.postgresConfigured())
      throw new IllegalStateException(
          "The failure lab's PostgreSQL is not configured. Start it with: docker compose"
              + " --profile failure-lab up -d");
  }

  /** A connection to database {@code name} on the lab server (the configured one when null). */
  public Connection connect(String name) throws SQLException {
    requireConfigured();
    String url = settings.postgresUrl();
    if (name != null) url = url.replaceFirst("/[^/?]+(\\?|$)", "/" + check(name) + "$1");
    var connection =
        DriverManager.getConnection(url, settings.postgresUser(), settings.postgresPassword());
    connection.setAutoCommit(true);
    return connection;
  }

  /** The JDBC URL of a database, for the coordinator process. */
  public String url(String name) {
    requireConfigured();
    return settings.postgresUrl().replaceFirst("/[^/?]+(\\?|$)", "/" + check(name) + "$1");
  }

  public String user() {
    return settings.postgresUser();
  }

  public String password() {
    return settings.postgresPassword();
  }

  public boolean exists(String name) throws SQLException {
    try (var c = connect(null);
        var s = c.prepareStatement("SELECT 1 FROM pg_database WHERE datname = ?")) {
      s.setString(1, check(name));
      try (var rs = s.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Drops a database after ending every session on it (FORCE). Refused by PostgreSQL while a
   * prepared transaction lives in it: callers resolve those first.
   */
  public void drop(String name) throws SQLException {
    try (var c = connect(null);
        var s = c.createStatement()) {
      s.execute("DROP DATABASE IF EXISTS " + check(name) + " WITH (FORCE)");
    }
  }

  public void create(String name) throws SQLException {
    try (var c = connect(null);
        var s = c.createStatement()) {
      s.execute("CREATE DATABASE " + check(name));
    }
  }

  /**
   * A file-level copy of {@code source}: CREATE DATABASE … TEMPLATE. PostgreSQL requires that
   * nobody is connected to the source while it copies.
   */
  public void copy(String source, String target) throws SQLException {
    try (var c = connect(null);
        var s = c.createStatement()) {
      s.execute(
          "CREATE DATABASE "
              + check(target)
              + " TEMPLATE "
              + check(source)
              + " STRATEGY FILE_COPY");
    }
  }

  public long size(String name) throws SQLException {
    try (var c = connect(null);
        var s = c.prepareStatement("SELECT pg_database_size(?)")) {
      s.setString(1, check(name));
      try (var rs = s.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  /** Runs SQL statements (DDL) one by one on a database. */
  public void execute(String database, String... statements) throws SQLException {
    try (var c = connect(database);
        var s = c.createStatement()) {
      for (var sql : statements) s.execute(sql);
    }
  }

  /** Every row of a query as ordered maps, for step results. */
  public static List<Map<String, Object>> rows(Connection c, String sql, Object... args)
      throws SQLException {
    try (var s = c.prepareStatement(sql)) {
      for (int i = 0; i < args.length; i++) s.setObject(i + 1, args[i]);
      try (var rs = s.executeQuery()) {
        return rows(rs);
      }
    }
  }

  public static List<Map<String, Object>> rows(ResultSet rs) throws SQLException {
    var result = new ArrayList<Map<String, Object>>();
    var meta = rs.getMetaData();
    while (rs.next()) {
      var row = new LinkedHashMap<String, Object>();
      for (int i = 1; i <= meta.getColumnCount(); i++) {
        var value = rs.getObject(i);
        if (value instanceof java.sql.Timestamp t) value = t.toInstant().toString();
        else if (value instanceof java.sql.Array a) value = List.of((Object[]) a.getArray());
        else if (value instanceof java.time.OffsetDateTime t) value = t.toInstant().toString();
        else if (value != null
            && !(value instanceof Number)
            && !(value instanceof Boolean)
            && !(value instanceof String)) value = value.toString();
        row.put(meta.getColumnLabel(i), value);
      }
      result.add(row);
    }
    return result;
  }

  public static Map<String, Object> one(Connection c, String sql, Object... args)
      throws SQLException {
    var all = rows(c, sql, args);
    return all.isEmpty() ? Map.of() : all.getFirst();
  }

  static String check(String name) {
    if (!NAME.matcher(name).matches())
      throw new IllegalArgumentException("Not a failure-lab database: " + name);
    return name;
  }
}
