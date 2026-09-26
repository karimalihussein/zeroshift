package io.zeroshift.racelab.infrastructure.postgres;

import io.zeroshift.racelab.application.port.Session;
import io.zeroshift.racelab.domain.Isolation;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.postgresql.util.PSQLException;

/** A request's own pooled connection, named in pg_stat_activity so its locks can be traced. */
final class JdbcSession implements Session {
  private final Connection connection;
  private final int pid;

  JdbcSession(Connection connection, String applicationName) {
    this.connection = connection;
    try {
      connection.setAutoCommit(true);
      try (var ps =
          connection.prepareStatement("SELECT set_config('application_name', ?, false)")) {
        ps.setString(1, applicationName);
        ps.execute();
      }
      try (var st = connection.createStatement();
          var rs = st.executeQuery("SELECT pg_backend_pid()")) {
        rs.next();
        pid = rs.getInt(1);
      }
    } catch (SQLException e) {
      close();
      throw failure(e);
    }
  }

  @Override
  public int pid() {
    return pid;
  }

  @Override
  public void begin(Isolation isolation) {
    try {
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(isolation.jdbc());
    } catch (SQLException e) {
      throw failure(e);
    }
  }

  @Override
  public long txId() {
    var rows = query("SELECT pg_current_xact_id()::text AS xid", List.of());
    return Long.parseLong(rows.getFirst().get("xid").toString());
  }

  @Override
  public List<Map<String, Object>> query(String sql, List<Object> params) {
    try (var ps = prepare(sql, params);
        var rs = ps.executeQuery()) {
      var meta = rs.getMetaData();
      var rows = new ArrayList<Map<String, Object>>();
      while (rs.next()) {
        var row = new LinkedHashMap<String, Object>();
        for (int i = 1; i <= meta.getColumnCount(); i++)
          row.put(meta.getColumnLabel(i), rs.getObject(i));
        rows.add(row);
      }
      return rows;
    } catch (SQLException e) {
      throw failure(e);
    }
  }

  @Override
  public int update(String sql, List<Object> params) {
    try (var ps = prepare(sql, params)) {
      return ps.executeUpdate();
    } catch (SQLException e) {
      throw failure(e);
    }
  }

  @Override
  public void commit() {
    try {
      connection.commit();
    } catch (SQLException e) {
      throw failure(e);
    }
  }

  @Override
  public void rollback() {
    try {
      connection.rollback();
    } catch (SQLException e) {
      throw failure(e);
    }
  }

  @Override
  public void close() {
    try {
      if (!connection.getAutoCommit()) connection.rollback();
      connection.setAutoCommit(true);
      try (var ps =
          connection.prepareStatement(
              "SELECT set_config('application_name', 'race-lab idle', false)")) {
        ps.execute();
      }
    } catch (SQLException ignored) {
      // the pool validates the connection before handing it out again
    } finally {
      try {
        connection.close();
      } catch (SQLException ignored) {
        // nothing more to release
      }
    }
  }

  private PreparedStatement prepare(String sql, List<Object> params) throws SQLException {
    var ps = connection.prepareStatement(sql);
    for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
    return ps;
  }

  static DbFailure failure(SQLException e) {
    String state = e.getSQLState();
    String message = e.getMessage();
    String detail = null;
    if (e instanceof PSQLException p && p.getServerErrorMessage() != null) {
      message = p.getServerErrorMessage().getMessage();
      detail = p.getServerErrorMessage().getDetail();
    }
    var kind =
        switch (state == null ? "" : state) {
          case "40001" -> DbFailure.Kind.SERIALIZATION;
          case "40P01" -> DbFailure.Kind.DEADLOCK;
          case "55P03" -> DbFailure.Kind.LOCK_NOT_AVAILABLE;
          case "57014" -> DbFailure.Kind.CANCELED;
          default -> DbFailure.Kind.OTHER;
        };
    return new DbFailure(kind, state, message, detail, e);
  }
}
