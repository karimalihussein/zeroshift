package io.zeroshift.infrastructure;

import static io.zeroshift.infrastructure.ReverseSyncWriter.*;

import com.zaxxer.hikari.HikariDataSource;
import io.zeroshift.application.port.SourceWriteback;
import io.zeroshift.domain.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@DependsOn("schemaInitializer")
public class SqlServerWriteback implements SourceWriteback {
  private final DataSource source;
  private final JdbcTemplate jdbc;

  public SqlServerWriteback(@Qualifier("sourceDataSource") DataSource source) {
    this.source = source;
    jdbc = new JdbcTemplate(source);
    jdbc.setQueryTimeout(60);
  }

  @Override
  public List<ReverseConflict> apply(Table table, List<Change> changes, long baselineVersion) {
    if (changes.isEmpty()) return List.of();
    String name = "dbo." + table.sqlName();
    // Keys are longs from our own capture, so an inline IN list cannot inject.
    String ids = changes.stream().map(c -> Long.toString(c.id())).collect(Collectors.joining(","));
    Connection connection = null;
    boolean identityInsert = false;
    try {
      connection = source.getConnection();
      connection.setAutoCommit(false);
      execute(
          connection,
          "EXEC sp_set_session_context @key=N'"
              + SESSION_KEY
              + "', @value=N'"
              + SESSION_VALUE
              + "'");
      // Key and key-range locks first: a write outside reverse sync either committed before this
      // point, and is visible in Change Tracking below, or waits until this transaction ends.
      execute(
          connection,
          "SELECT id FROM " + name + " WITH (UPDLOCK,HOLDLOCK) WHERE id IN (" + ids + ")");
      var conflicts = new ArrayList<ReverseConflict>();
      try (var statement =
          connection.prepareStatement(
              "SELECT ct.id FROM CHANGETABLE(CHANGES "
                  + name
                  + ",?) ct WHERE ct.id IN ("
                  + ids
                  + ") AND (ct.SYS_CHANGE_CONTEXT IS NULL OR ct.SYS_CHANGE_CONTEXT<>"
                  + CHANGE_CONTEXT
                  + ") ORDER BY ct.id")) {
        statement.setLong(1, baselineVersion);
        try (var rs = statement.executeQuery()) {
          while (rs.next()) conflicts.add(new ReverseConflict(table, rs.getLong(1)));
        }
      }
      if (!conflicts.isEmpty()) {
        connection.rollback();
        return conflicts;
      }
      var upserts = changes.stream().filter(c -> c.operation() != Change.Operation.DELETE).toList();
      if (!upserts.isEmpty()) {
        execute(connection, "SET IDENTITY_INSERT " + name + " ON");
        identityInsert = true;
        try (var statement = connection.prepareStatement(merge(table))) {
          for (var change : upserts) {
            bind(statement, change.row());
            statement.addBatch();
          }
          statement.executeBatch();
        }
        execute(connection, "SET IDENTITY_INSERT " + name + " OFF");
        identityInsert = false;
      }
      var deletes = changes.stream().filter(c -> c.operation() == Change.Operation.DELETE).toList();
      if (!deletes.isEmpty()) {
        try (var statement =
            connection.prepareStatement(
                "WITH CHANGE_TRACKING_CONTEXT("
                    + CHANGE_CONTEXT
                    + ") DELETE "
                    + name
                    + " WHERE id=?")) {
          for (var change : deletes) {
            statement.setLong(1, change.id());
            statement.addBatch();
          }
          statement.executeBatch();
        }
      }
      connection.commit();
      return List.of();
    } catch (SQLException e) {
      rollbackQuietly(connection, e);
      throw new MigrationException("Reverse sync into SQL Server failed for " + table.sqlName(), e);
    } finally {
      release(connection, name, identityInsert);
    }
  }

  @Override
  public void synchronizeIdentities(Map<Table, Long> issuedByPostgres) {
    for (var table : Table.values()) {
      String name = "dbo." + table.sqlName();
      long current =
          Objects.requireNonNull(
              jdbc.queryForObject(
                  "SELECT CAST(GREATEST(ISNULL(IDENT_CURRENT('"
                      + name
                      + "'),0),ISNULL((SELECT MAX(id) FROM "
                      + name
                      + "),0)) AS BIGINT)",
                  Long.class));
      long floor = Math.max(current, issuedByPostgres.getOrDefault(table, 0L));
      // RESEED with a value never lowers the seed here: floor is at least the current identity.
      if (floor > 0)
        jdbc.execute("DBCC CHECKIDENT ('" + name + "', RESEED, " + floor + ") WITH NO_INFOMSGS");
    }
  }

  private static String merge(Table table) {
    String columns = Rows.columns(table);
    String update =
        table == Table.CUSTOMERS
            ? "name=s.name,email=s.email,active=s.active"
            : "customer_id=s.customer_id,amount=s.amount,status=s.status";
    return "WITH CHANGE_TRACKING_CONTEXT("
        + CHANGE_CONTEXT
        + ") MERGE dbo."
        + table.sqlName()
        + " WITH (HOLDLOCK) AS t USING (VALUES(?,?,?,?)) AS s("
        + columns
        + ") ON t.id=s.id WHEN MATCHED THEN UPDATE SET "
        + update
        + " WHEN NOT MATCHED THEN INSERT("
        + columns
        + ") VALUES(s."
        + columns.replace(",", ",s.")
        + ");";
  }

  private static void bind(PreparedStatement statement, Row row) throws SQLException {
    statement.setLong(1, row.id());
    switch (row) {
      case Row.Customer c -> {
        statement.setNString(2, c.name());
        if (c.email() == null) statement.setNull(3, Types.NVARCHAR);
        else statement.setNString(3, c.email());
        statement.setBoolean(4, c.active());
      }
      case Row.Order o -> {
        statement.setLong(2, o.customerId());
        statement.setBigDecimal(3, o.amount());
        statement.setString(4, o.status());
      }
    }
  }

  private static void execute(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.setQueryTimeout(60);
      statement.execute(sql);
    }
  }

  private static void rollbackQuietly(Connection connection, SQLException cause) {
    if (connection == null) return;
    try {
      connection.rollback();
    } catch (SQLException suppressed) {
      cause.addSuppressed(suppressed);
    }
  }

  /** A pooled connection must never keep the fence bypass or IDENTITY_INSERT. */
  private void release(Connection connection, String name, boolean identityInsert) {
    if (connection == null) return;
    try {
      if (identityInsert) execute(connection, "SET IDENTITY_INSERT " + name + " OFF");
      execute(connection, "EXEC sp_set_session_context @key=N'" + SESSION_KEY + "', @value=NULL");
      connection.setAutoCommit(true);
      connection.close();
    } catch (SQLException e) {
      if (source instanceof HikariDataSource pool) pool.evictConnection(connection);
      else
        try {
          connection.close();
        } catch (SQLException ignored) {
          // The connection is already unusable; the pool discards it.
        }
    }
  }
}
