package io.zeroshift.infrastructure;

import io.zeroshift.application.port.SourceDatabase;
import io.zeroshift.domain.*;
import java.sql.*;
import java.util.*;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

@Repository
@DependsOn("schemaInitializer")
public class SqlServerReader implements SourceDatabase {
  private final DataSource source;
  private static final int BULK_CHUNK = 100_000;
  private final JdbcTemplate jdbc;
  // Seeding and reset touch millions of rows; each chunk commits separately under a long timeout.
  private final JdbcTemplate bulk;
  private final TransactionTemplate transactions;

  public SqlServerReader(@Qualifier("sourceDataSource") DataSource source) {
    this.source = source;
    jdbc = new JdbcTemplate(source);
    jdbc.setQueryTimeout(30);
    bulk = new JdbcTemplate(source);
    bulk.setQueryTimeout(600);
    transactions = new TransactionTemplate(new DataSourceTransactionManager(source));
  }

  @Override
  public Boundary boundary() {
    try (var capture = new ChangeTrackingCapture(source, null)) {
      return capture.boundary();
    }
  }

  @Override
  public List<Row> read(Table table, long afterId, long upperId, int limit) {
    return jdbc.query(
        "SELECT TOP (?) "
            + Rows.columns(table)
            + " FROM dbo."
            + table.sqlName()
            + " WHERE id>? AND id<=? ORDER BY id",
        (rs, n) -> Rows.read(table, rs),
        limit,
        afterId,
        upperId);
  }

  @Override
  public Capture capture(long afterVersion) {
    return new ChangeTrackingCapture(source, afterVersion);
  }

  @Override
  public void freeze(boolean frozen) {
    jdbc.update("UPDATE dbo.migration_gate SET frozen=? WHERE id=1", frozen);
  }

  @Override
  public long count(Table table) {
    return Objects.requireNonNull(
        jdbc.queryForObject("SELECT COUNT_BIG(*) FROM dbo." + table.sqlName(), Long.class));
  }

  @Override
  public void reset() {
    jdbc.update("UPDATE dbo.migration_gate SET frozen=0 WHERE id=1");
    jdbc.update("DELETE dbo.live_experiment");
    while (bulk.update("DELETE TOP (" + BULK_CHUNK + ") dbo.orders") > 0) {}
    while (bulk.update("DELETE TOP (" + BULK_CHUNK + ") dbo.customers") > 0) {}
    transactions.executeWithoutResult(
        s -> {
          // SQL Server reseeds a never-used empty identity differently from a deleted table.
          // Materialize and remove one pair so the next generated key is always 1, never 0.
          jdbc.update("INSERT dbo.customers(name,email,active) VALUES('Reset marker',NULL,1)");
          jdbc.update(
              "INSERT dbo.orders(customer_id,amount,status) SELECT id,0,'RESET' FROM dbo.customers");
          jdbc.update("DELETE dbo.orders");
          jdbc.update("DELETE dbo.customers");
          jdbc.execute("DBCC CHECKIDENT ('dbo.orders', RESEED, 0) WITH NO_INFOMSGS");
          jdbc.execute("DBCC CHECKIDENT ('dbo.customers', RESEED, 0) WITH NO_INFOMSGS");
        });
  }

  /** Generates rows set-based inside SQL Server; a failure leaves committed chunks for Reset. */
  @Override
  public void seed(int rows) {
    for (int base = 0; base < rows; base += BULK_CHUNK) {
      int offset = base;
      int size = Math.min(BULK_CHUNK, rows - base);
      transactions.executeWithoutResult(
          s -> {
            long before =
                Objects.requireNonNull(
                    jdbc.queryForObject("SELECT ISNULL(MAX(id),0) FROM dbo.customers", Long.class));
            bulk.update(
                "INSERT dbo.customers(name,email,active) SELECT CONCAT(N'Customer ',n,N' · عميل'),"
                    + "CASE WHEN n%7=0 THEN NULL ELSE CONCAT(N'customer',n,N'@example.test') END,1"
                    + " FROM (SELECT TOP (?) CAST(?+ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS BIGINT) n"
                    + " FROM sys.all_columns a CROSS JOIN sys.all_columns b) t ORDER BY n",
                size, offset);
            bulk.update(
                "INSERT dbo.orders(customer_id,amount,status) SELECT id,CAST((id%10000)/10.0 AS DECIMAL(19,4)),'NEW'"
                    + " FROM dbo.customers WHERE id>? ORDER BY id",
                before);
          });
    }
  }

  @Override
  public TrafficOperationOutcome writeTraffic(TrafficOperation operation) {
    return transactions.execute(
        s -> {
          return switch (operation) {
            case INSERT -> {
              long customerId =
                  jdbc.queryForObject(
                      "SET NOCOUNT ON; DECLARE @ids TABLE(id BIGINT); INSERT dbo.customers(name,email,active) OUTPUT INSERTED.id INTO @ids VALUES(N'Live customer · عميل',NULL,1); SELECT id FROM @ids",
                      Long.class);
              long orderId =
                  jdbc.queryForObject(
                      "SET NOCOUNT ON; DECLARE @ids TABLE(id BIGINT); INSERT dbo.orders(customer_id,amount,status) OUTPUT INSERTED.id INTO @ids VALUES(?,12.3456,'NEW'); SELECT id FROM @ids",
                      Long.class,
                      customerId);
              yield new TrafficOperationOutcome(
                  Table.CUSTOMERS, customerId, "order #" + orderId + " created");
            }
            case UPDATE -> {
              var order = randomRow("orders", "id,customer_id");
              if (order.isEmpty())
                yield new TrafficOperationOutcome(Table.ORDERS, null, "no rows available");
              long orderId = id(order.get(), "id");
              long customerId = id(order.get(), "customer_id");
              jdbc.update(
                  "UPDATE dbo.orders SET amount=amount+1.0001,status='UPDATED' WHERE id=?",
                  orderId);
              jdbc.update(
                  "UPDATE dbo.customers SET active=CASE active WHEN 1 THEN 0 ELSE 1 END WHERE id=?",
                  customerId);
              yield new TrafficOperationOutcome(
                  Table.ORDERS, orderId, "status=UPDATED · customer #" + customerId + " toggled");
            }
            case DELETE -> {
              var customer = randomRow("customers", "id");
              if (customer.isEmpty())
                yield new TrafficOperationOutcome(Table.CUSTOMERS, null, "no rows available");
              long customerId = id(customer.get(), "id");
              int orders = jdbc.update("DELETE dbo.orders WHERE customer_id=?", customerId);
              jdbc.update("DELETE dbo.customers WHERE id=?", customerId);
              yield new TrafficOperationOutcome(
                  Table.CUSTOMERS, customerId, orders + " related orders deleted");
            }
            case READ -> {
              var order = randomRow("orders", "id");
              if (order.isEmpty())
                yield new TrafficOperationOutcome(Table.ORDERS, null, "no rows available");
              long orderId = id(order.get(), "id");
              var row =
                  jdbc.queryForMap(
                      "SELECT o.amount,o.status FROM dbo.orders o WHERE o.id=?", orderId);
              yield new TrafficOperationOutcome(
                  Table.ORDERS,
                  orderId,
                  "status=" + row.get("status") + " · amount=" + row.get("amount"));
            }
          };
        });
  }

  private static long id(Map<String, Object> row, String column) {
    return ((Number) row.get(column)).longValue();
  }

  /** Seeks the first key at a uniformly random point of the id range; no full scan. */
  private Optional<Map<String, Object>> randomRow(String table, String columns) {
    return jdbc
        .queryForList(
            "SELECT TOP (1) "
                + columns
                + " FROM dbo."
                + table
                + " WHERE id>=(SELECT MIN(id)+CAST(FLOOR(RAND()*(MAX(id)-MIN(id)+1)) AS BIGINT) FROM dbo."
                + table
                + ") ORDER BY id")
        .stream()
        .findFirst();
  }
}
