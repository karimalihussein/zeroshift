package io.zeroshift.infrastructure;

import io.zeroshift.application.port.SourceDatabase;
import io.zeroshift.domain.*;
import java.sql.*;
import java.util.*;
import javax.sql.DataSource;

/** Retention check, high watermark and row values share one snapshot: no commit-order gaps. */
final class ChangeTrackingCapture implements SourceDatabase.Capture {
  private final Connection connection;
  private final long after;
  private final long version;

  ChangeTrackingCapture(DataSource dataSource, Long afterVersion) {
    Connection opened = null;
    try {
      opened = dataSource.getConnection();
      connection = opened;
      connection.setTransactionIsolation(
          com.microsoft.sqlserver.jdbc.SQLServerConnection.TRANSACTION_SNAPSHOT);
      connection.setAutoCommit(false);
      // Metadata functions alone do not establish SQL Server's data snapshot. Touch a user
      // table first, otherwise later row reads can observe commits beyond the chosen watermark.
      scalar("SELECT CAST(frozen AS BIGINT) FROM dbo.migration_gate WHERE id=1");
      // Validate retention before choosing the high watermark, all inside one snapshot.
      for (var table : Table.values()) {
        long minimum =
            scalar(
                "SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('dbo."
                    + table.sqlName()
                    + "'))");
        if (afterVersion != null && afterVersion < minimum) throw new CaptureExpired(afterVersion);
      }
      version = scalar("SELECT CHANGE_TRACKING_CURRENT_VERSION()");
      after = afterVersion == null ? version : afterVersion;
      if (after > version)
        throw new MigrationException("Source change version moved backwards; reset the migration");
    } catch (Exception e) {
      if (opened != null)
        try {
          opened.close();
        } catch (SQLException suppressed) {
          e.addSuppressed(suppressed);
        }
      if (e instanceof MigrationException failure) throw failure;
      throw new MigrationException("Cannot open change capture", e);
    }
  }

  private long scalar(String sql) {
    try (var statement = connection.createStatement()) {
      statement.setQueryTimeout(30);
      try (var rs = statement.executeQuery(sql)) {
        rs.next();
        long value = rs.getLong(1);
        if (rs.wasNull())
          throw new MigrationException(
              "Change Tracking is unavailable; reset after restoring capture");
        return value;
      }
    } catch (SQLException e) {
      throw new MigrationException("Cannot read capture boundary", e);
    }
  }

  SourceDatabase.Boundary boundary() {
    return new SourceDatabase.Boundary(
        version,
        scalar("SELECT COALESCE(MAX(id),0) FROM dbo.customers"),
        scalar("SELECT COALESCE(MAX(id),0) FROM dbo.orders"),
        scalar("SELECT COUNT_BIG(*) FROM dbo.customers")
            + scalar("SELECT COUNT_BIG(*) FROM dbo.orders"));
  }

  @Override
  public long version() {
    return version;
  }

  @Override
  public long pending() {
    long count = 0;
    for (var table : Table.values())
      count +=
          scalar(
              "SELECT COUNT_BIG(*) FROM CHANGETABLE(CHANGES dbo."
                  + table.sqlName()
                  + ","
                  + after
                  + ") AS ct");
    return count;
  }

  @Override
  public List<Long> outOfBand(Table table) {
    String sql =
        "SELECT ct.id FROM CHANGETABLE(CHANGES dbo."
            + table.sqlName()
            + ",?) ct WHERE ct.SYS_CHANGE_CONTEXT IS NULL OR ct.SYS_CHANGE_CONTEXT<>"
            + ReverseSyncWriter.CHANGE_CONTEXT
            + " ORDER BY ct.id";
    try (var statement = connection.prepareStatement(sql)) {
      statement.setQueryTimeout(30);
      statement.setLong(1, after);
      try (var rs = statement.executeQuery()) {
        List<Long> ids = new ArrayList<>();
        while (rs.next()) ids.add(rs.getLong(1));
        return ids;
      }
    } catch (SQLException e) {
      throw new MigrationException("Cannot check " + table + " for writes outside reverse sync", e);
    }
  }

  @Override
  public List<Change> read(Table table, long afterId, int limit) {
    String columns =
        table == Table.CUSTOMERS ? "r.name,r.email,r.active" : "r.customer_id,r.amount,r.status";
    String sql =
        "SELECT TOP (?) ct.id,ct.SYS_CHANGE_OPERATION,ct.SYS_CHANGE_VERSION,"
            + columns
            + " FROM CHANGETABLE(CHANGES dbo."
            + table.sqlName()
            + ",?) ct LEFT JOIN dbo."
            + table.sqlName()
            + " r ON r.id=ct.id WHERE ct.id>? ORDER BY ct.id";
    try (var statement = connection.prepareStatement(sql)) {
      statement.setQueryTimeout(30);
      statement.setInt(1, limit);
      statement.setLong(2, after);
      statement.setLong(3, afterId);
      try (var rs = statement.executeQuery()) {
        List<Change> changes = new ArrayList<>();
        while (rs.next()) {
          boolean deleted = "D".equals(rs.getString("SYS_CHANGE_OPERATION"));
          changes.add(
              new Change(
                  rs.getLong("id"),
                  deleted
                      ? Change.Operation.DELETE
                      : "I".equals(rs.getString("SYS_CHANGE_OPERATION"))
                          ? Change.Operation.INSERT
                          : Change.Operation.UPDATE,
                  deleted ? null : Rows.read(table, rs),
                  rs.getLong("SYS_CHANGE_VERSION")));
        }
        return changes;
      }
    } catch (SQLException e) {
      throw new MigrationException("Cannot read changes for " + table, e);
    }
  }

  @Override
  public void close() {
    try (connection) {
      connection.rollback();
      connection.setAutoCommit(true);
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    } catch (SQLException e) {
      throw new MigrationException("Cannot close capture transaction", e);
    }
  }
}
