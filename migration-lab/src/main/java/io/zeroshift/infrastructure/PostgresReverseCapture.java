package io.zeroshift.infrastructure;

import io.zeroshift.application.port.MigrationStore;
import io.zeroshift.domain.*;
import java.sql.*;
import java.util.*;
import javax.sql.DataSource;

/** One REPEATABLE READ snapshot: captured keys and their current rows are read consistently. */
final class PostgresReverseCapture implements MigrationStore.ReverseCapture {
  private final Connection connection;

  PostgresReverseCapture(DataSource dataSource) {
    Connection opened = null;
    try {
      opened = dataSource.getConnection();
      opened.setAutoCommit(false);
      opened.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      opened.setReadOnly(true);
      connection = opened;
    } catch (SQLException e) {
      if (opened != null)
        try {
          opened.close();
        } catch (SQLException suppressed) {
          e.addSuppressed(suppressed);
        }
      throw new MigrationException("Cannot open reverse capture", e);
    }
  }

  @Override
  public List<Change> changes(Table table, long afterId, int limit) {
    String sql =
        "SELECT c.record_id,c.version,c.op,r.* FROM (SELECT record_id,MAX(seq) AS version,"
            + "(ARRAY_AGG(operation ORDER BY seq DESC))[1] AS op FROM reverse_change"
            + " WHERE table_name=? AND record_id>? GROUP BY record_id ORDER BY record_id LIMIT ?) c"
            + " LEFT JOIN "
            + table.sqlName()
            + " r ON r.id=c.record_id ORDER BY c.record_id";
    try (var statement = connection.prepareStatement(sql)) {
      statement.setString(1, table.name());
      statement.setLong(2, afterId);
      statement.setInt(3, limit);
      try (var rs = statement.executeQuery()) {
        List<Change> changes = new ArrayList<>();
        while (rs.next()) {
          long id = rs.getLong("record_id");
          long version = rs.getLong("version");
          if (rs.getObject("id") == null)
            changes.add(new Change(id, Change.Operation.DELETE, null, version));
          else
            changes.add(
                new Change(
                    id,
                    "I".equals(rs.getString("op"))
                        ? Change.Operation.INSERT
                        : Change.Operation.UPDATE,
                    Rows.read(table, rs),
                    version));
        }
        return changes;
      }
    } catch (SQLException e) {
      throw new MigrationException("Cannot read captured " + table.sqlName() + " changes", e);
    }
  }

  @Override
  public List<Row> rows(Table table, long afterId, int limit) {
    try (var statement =
        connection.prepareStatement(
            "SELECT * FROM " + table.sqlName() + " WHERE id>? ORDER BY id LIMIT ?")) {
      statement.setLong(1, afterId);
      statement.setInt(2, limit);
      try (var rs = statement.executeQuery()) {
        List<Row> rows = new ArrayList<>();
        while (rs.next()) rows.add(Rows.read(table, rs));
        return rows;
      }
    } catch (SQLException e) {
      throw new MigrationException("Cannot read " + table.sqlName(), e);
    }
  }

  @Override
  public Set<Long> pendingKeys(Table table) {
    try (var statement =
        connection.prepareStatement(
            "SELECT DISTINCT record_id FROM reverse_change WHERE table_name=?")) {
      statement.setString(1, table.name());
      try (var rs = statement.executeQuery()) {
        Set<Long> keys = new HashSet<>();
        while (rs.next()) keys.add(rs.getLong(1));
        return keys;
      }
    } catch (SQLException e) {
      throw new MigrationException("Cannot read pending " + table.sqlName() + " keys", e);
    }
  }

  @Override
  public long pending() {
    try (var statement = connection.createStatement();
        var rs =
            statement.executeQuery(
                "SELECT COUNT(*) FROM (SELECT DISTINCT table_name,record_id FROM reverse_change) k")) {
      rs.next();
      return rs.getLong(1);
    } catch (SQLException e) {
      throw new MigrationException("Cannot count pending reverse changes", e);
    }
  }

  @Override
  public void close() {
    try (connection) {
      connection.rollback();
      connection.setReadOnly(false);
      connection.setAutoCommit(true);
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    } catch (SQLException e) {
      throw new MigrationException("Cannot close reverse capture", e);
    }
  }
}
