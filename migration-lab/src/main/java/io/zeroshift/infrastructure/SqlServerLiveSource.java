package io.zeroshift.infrastructure;

import io.zeroshift.application.port.LiveSource;
import io.zeroshift.domain.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

@Repository
@DependsOn("schemaInitializer")
public class SqlServerLiveSource implements LiveSource {
  private final JdbcTemplate jdbc;
  private final TransactionTemplate transactions;

  public SqlServerLiveSource(@Qualifier("sourceDataSource") DataSource dataSource) {
    jdbc = new JdbcTemplate(dataSource);
    jdbc.setQueryTimeout(30);
    transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    transactions.setIsolationLevel(java.sql.Connection.TRANSACTION_SERIALIZABLE);
  }

  @Override
  public Optional<OrderRecord> order(long id) {
    return jdbc
        .query(
            "SELECT o.id,o.customer_id,c.name,o.amount,o.status FROM dbo.orders o JOIN dbo.customers c ON c.id=o.customer_id WHERE o.id=?",
            (r, n) ->
                new OrderRecord(
                    r.getLong(1), r.getLong(2), r.getString(3), r.getBigDecimal(4), r.getString(5)),
            id)
        .stream()
        .findFirst();
  }

  @Override
  public Optional<LiveExperiment> latest(long orderId) {
    return jdbc
        .query(
            "SELECT TOP(1) * FROM dbo.live_experiment WHERE order_id=? ORDER BY id DESC",
            (r, n) ->
                new LiveExperiment(
                    r.getLong("id"),
                    orderId,
                    TrafficOperation.valueOf(r.getString("operation")),
                    record(r, "before"),
                    record(r, "after")),
            orderId)
        .stream()
        .findFirst();
  }

  private OrderRecord record(ResultSet r, String prefix) throws SQLException {
    String status = r.getString(prefix + "_status");
    return status == null
        ? null
        : new OrderRecord(
            r.getLong("order_id"),
            r.getLong("customer_id"),
            r.getString(prefix + "_name"),
            r.getBigDecimal(prefix + "_amount"),
            status);
  }

  @Override
  public LiveExperiment insert(OrderEdit edit) {
    return transactions.execute(
        s -> {
          long customerId =
              jdbc.queryForObject(
                  "SET NOCOUNT ON; DECLARE @ids TABLE(id BIGINT); INSERT dbo.customers(name,email,active) OUTPUT INSERTED.id INTO @ids VALUES(?,NULL,1); SELECT id FROM @ids",
                  Long.class,
                  edit.customerName());
          long id =
              jdbc.queryForObject(
                  "SET NOCOUNT ON; DECLARE @ids TABLE(id BIGINT); INSERT dbo.orders(customer_id,amount,status) OUTPUT INSERTED.id INTO @ids VALUES(?,?,?); SELECT id FROM @ids",
                  Long.class,
                  customerId,
                  edit.amount(),
                  edit.status());
          return save(TrafficOperation.INSERT, null, order(id).orElseThrow());
        });
  }

  @Override
  public LiveExperiment update(long id, OrderEdit edit) {
    return transactions.execute(
        s -> {
          var before =
              order(id)
                  .orElseThrow(
                      () ->
                          new InvalidAction(
                              "Order no longer exists in SQL Server; select another record"));
          // A status-only experiment changes exactly one tracked key, not its unchanged customer.
          if (!before.customerName().equals(edit.customerName()))
            jdbc.update(
                "UPDATE dbo.customers SET name=? WHERE id=?",
                edit.customerName(),
                before.customerId());
          if (before.amount().compareTo(edit.amount()) != 0
              || !before.status().equals(edit.status()))
            jdbc.update(
                "UPDATE dbo.orders SET amount=?,status=? WHERE id=?",
                edit.amount(),
                edit.status(),
                id);
          var after = order(id).orElseThrow();
          if (before.sameValues(after))
            throw new InvalidAction("Change at least one value before applying the update");
          return save(TrafficOperation.UPDATE, before, after);
        });
  }

  @Override
  public LiveExperiment delete(long id) {
    return transactions.execute(
        s -> {
          var before =
              order(id)
                  .orElseThrow(
                      () ->
                          new InvalidAction(
                              "Order no longer exists in SQL Server; select another record"));
          // Delete the leaf only; its customer and other orders remain valid.
          jdbc.update("DELETE dbo.orders WHERE id=?", id);
          return save(TrafficOperation.DELETE, before, null);
        });
  }

  private LiveExperiment save(TrafficOperation operation, OrderRecord before, OrderRecord after) {
    var row = after == null ? before : after;
    long id =
        jdbc.queryForObject(
            "SET NOCOUNT ON; INSERT dbo.live_experiment(order_id,customer_id,operation,before_name,before_amount,before_status,after_name,after_amount,after_status) VALUES(?,?,?,?,?,?,?,?,?); SELECT CAST(SCOPE_IDENTITY() AS BIGINT)",
            Long.class,
            row.id(),
            row.customerId(),
            operation.name(),
            before == null ? null : before.customerName(),
            before == null ? null : before.amount(),
            before == null ? null : before.status(),
            after == null ? null : after.customerName(),
            after == null ? null : after.amount(),
            after == null ? null : after.status());
    return new LiveExperiment(id, row.id(), operation, before, after);
  }
}
