package io.zeroshift.infrastructure;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import javax.sql.DataSource;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

@Repository
@DependsOn("schemaInitializer")
public class PostgresMigrationStore implements MigrationStore {
  private static final int MAX_RETAINED_LOGS = 1000;
  private static final DateTimeFormatter LOG_TIME =
      DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());
  private final JdbcTemplate jdbc;
  private final DataSource dataSource;
  private final TransactionTemplate transactions;
  private final PostgresBulkLoader loader = new PostgresBulkLoader();

  public PostgresMigrationStore(DataSource dataSource) {
    this.dataSource = dataSource;
    jdbc = new JdbcTemplate(dataSource);
    jdbc.setQueryTimeout(60);
    transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
  }

  @Override
  public MigrationState state() {
    return readState(false);
  }

  private MigrationState readState(boolean lock) {
    return jdbc.queryForObject(
        "SELECT * FROM migration_state WHERE id=1" + (lock ? " FOR UPDATE" : ""),
        (r, n) ->
            new MigrationState(
                Stage.valueOf(r.getString("stage")),
                RunStatus.valueOf(r.getString("status")),
                Primary.valueOf(r.getString("primary_db")),
                Table.valueOf(r.getString("current_table")),
                r.getLong("last_id"),
                r.getLong("customer_bound"),
                r.getLong("order_bound"),
                r.getLong("version"),
                r.getLong("copied"),
                r.getLong("expected"),
                r.getLong("batches"),
                r.getLong("applied"),
                r.getBoolean("traffic"),
                r.getString("validation"),
                r.getString("error"),
                r.getTimestamp("checkpoint") == null
                    ? null
                    : r.getTimestamp("checkpoint").toInstant(),
                r.getDouble("rows_per_second"),
                r.getBoolean("cdc_paused")));
  }

  @Override
  public <T> T transaction(Function<Session, T> work) {
    return transactions.execute(
        s -> {
          readState(true); // A durable row lock serializes worker, router and operator commands.
          return work.apply(new PgSession());
        });
  }

  @Override
  public List<String> logs() {
    return jdbc.query(
        "SELECT at,message FROM migration_log ORDER BY id DESC LIMIT 60",
        (r, n) -> "[" + LOG_TIME.format(r.getTimestamp(1).toInstant()) + "] " + r.getString(2));
  }

  @Override
  public long count(Table table) {
    return Objects.requireNonNull(
        jdbc.queryForObject("SELECT COUNT(*) FROM " + table.sqlName(), Long.class));
  }

  @Override
  public List<Row> read(Table table, long afterId, int limit) {
    return jdbc.query(
        "SELECT * FROM " + table.sqlName() + " WHERE id>? ORDER BY id LIMIT ?",
        (r, n) -> Rows.read(table, r),
        afterId,
        limit);
  }

  @Override
  public TrafficMetrics trafficMetrics() {
    // A window with no committed operation for 3s means traffic is stalled, not still flowing.
    return jdbc.queryForObject(
        "SELECT s.traffic,s.primary_db,m.*,CASE WHEN s.traffic AND now()-m.window_started<interval '3 seconds' THEN m.ops_per_second ELSE 0 END AS rate FROM traffic_metrics m CROSS JOIN migration_state s WHERE m.id=1 AND s.id=1",
        (r, n) ->
            new TrafficMetrics(
                r.getBoolean("traffic"),
                Primary.valueOf(r.getString("primary_db")),
                r.getLong("inserts")
                    + r.getLong("updates")
                    + r.getLong("deletes")
                    + r.getLong("reads"),
                r.getLong("inserts"),
                r.getLong("updates"),
                r.getLong("deletes"),
                r.getLong("reads"),
                r.getLong("errors"),
                r.getLong("sql_server_ops"),
                r.getLong("postgres_ops"),
                r.getDouble("rate")));
  }

  @Override
  public long appliedVersion(Table table, long id) {
    return jdbc
        .query(
            "SELECT version FROM replay_receipt WHERE table_name=? AND record_id=?",
            (r, n) -> r.getLong(1),
            table.name(),
            id)
        .stream()
        .findFirst()
        .orElse(0L);
  }

  private final class PgSession implements Session {
    @Override
    public MigrationState state() {
      return readState(false);
    }

    @Override
    public void start(SourceDatabase.Boundary b) {
      jdbc.update(
          "UPDATE migration_state SET stage='SNAPSHOT',status='RUNNING',version=?,customer_bound=?,order_bound=?,expected=?,checkpoint=now() WHERE id=1",
          b.version(),
          b.customers(),
          b.orders(),
          b.count());
      log("Change Tracking boundary " + b.version() + " captured; keyset snapshot started");
    }

    @Override
    public void stage(Stage stage) {
      jdbc.update("UPDATE migration_state SET stage=?,checkpoint=now() WHERE id=1", stage.name());
      log("Stage: " + stage);
    }

    @Override
    public void status(RunStatus status, String error) {
      jdbc.update(
          "UPDATE migration_state SET status=?,error=?,crash_requested=FALSE,rows_per_second=0 WHERE id=1",
          status.name(),
          error);
      log(status + (error.isBlank() ? "" : ": " + error));
    }

    @Override
    public void requestCrash() {
      jdbc.update("UPDATE migration_state SET crash_requested=TRUE WHERE id=1");
      log("Crash armed: next transaction will fail before its checkpoint");
    }

    @Override
    public void checkCrash() {
      if (Boolean.TRUE.equals(
          jdbc.queryForObject(
              "SELECT crash_requested FROM migration_state WHERE id=1", Boolean.class)))
        throw new SimulatedCrash();
    }

    @Override
    public void snapshot(List<Row> rows, long lastId, long startedNanos) {
      loader.upsert(DataSourceUtils.getConnection(dataSource), state().table(), rows);
      checkCrash();
      jdbc.update(
          "UPDATE migration_state SET last_id=?,copied=copied+?,batches=batches+1,checkpoint=now(),rows_per_second=? WHERE id=1",
          lastId,
          rows.size(),
          rows.size() / Math.max(0.001, (System.nanoTime() - startedNanos) / 1_000_000_000.0));
      log("COPY " + rows.size() + " " + state().table().sqlName() + "; committed key " + lastId);
    }

    @Override
    public void nextTable() {
      if (state().table() == Table.CUSTOMERS) {
        jdbc.update("UPDATE migration_state SET current_table='ORDERS',last_id=0 WHERE id=1");
        log("Snapshot: orders");
      } else stage(Stage.CATCH_UP);
    }

    @Override
    public long apply(Table table, List<Change> changes) {
      // Receipts and row changes share this transaction; a rollback cannot acknowledge a lost
      // write.
      changes =
          changes.stream()
              .filter(
                  c ->
                      !jdbc.query(
                              "INSERT INTO replay_receipt(table_name,record_id,version) VALUES(?,?,?) ON CONFLICT(table_name,record_id) DO UPDATE SET version=EXCLUDED.version WHERE replay_receipt.version<EXCLUDED.version RETURNING version",
                              (r, n) -> r.getLong(1),
                              table.name(),
                              c.id(),
                              c.version())
                          .isEmpty())
              .toList();
      loader.upsert(
          DataSourceUtils.getConnection(dataSource),
          table,
          changes.stream()
              .filter(c -> c.operation() != Change.Operation.DELETE)
              .map(Change::row)
              .toList());
      jdbc.batchUpdate(
          "DELETE FROM " + table.sqlName() + " WHERE id=?",
          changes.stream().filter(c -> c.operation() == Change.Operation.DELETE).toList(),
          1000,
          (p, c) -> p.setLong(1, c.id()));
      return changes.size();
    }

    @Override
    public void cdcPaused(boolean paused) {
      jdbc.update("UPDATE migration_state SET cdc_paused=? WHERE id=1", paused);
      log(paused ? "CDC replay paused; snapshot and source writes continue" : "CDC replay resumed");
    }

    @Override
    public void captured(long version, long applied) {
      checkCrash();
      jdbc.update(
          "UPDATE migration_state SET version=?,applied=applied+?,checkpoint=now(),rows_per_second=0 WHERE id=1",
          version,
          applied);
      if (applied > 0) log("Applied " + applied + " net changes through version " + version);
    }

    @Override
    public void prepare() {
      jdbc.execute("CREATE INDEX IF NOT EXISTS orders_customer_idx ON orders(customer_id)");
      jdbc.execute(
          "DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='orders_customer_fk') THEN ALTER TABLE orders ADD CONSTRAINT orders_customer_fk FOREIGN KEY(customer_id) REFERENCES customers(id) DEFERRABLE INITIALLY DEFERRED NOT VALID; END IF; IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='orders_amount_check') THEN ALTER TABLE orders ADD CONSTRAINT orders_amount_check CHECK(amount>=0) NOT VALID; END IF; END $$");
      synchronizeSequences();
      jdbc.execute("ANALYZE customers");
      jdbc.execute("ANALYZE orders");
      log("Built index and deferred constraints; synchronized sequences; ANALYZE complete");
    }

    @Override
    public void synchronizeSequences() {
      for (var table : Table.values())
        jdbc.execute(
            "SELECT setval(pg_get_serial_sequence('"
                + table.sqlName()
                + "','id'),COALESCE(MAX(id),1),MAX(id) IS NOT NULL) FROM "
                + table.sqlName());
    }

    @Override
    public void validation(ValidationResult result) {
      if (result.matches()) {
        // Flush deferred FK trigger events before ALTER TABLE validation.
        jdbc.execute("SET CONSTRAINTS ALL IMMEDIATE");
        jdbc.execute("ALTER TABLE orders VALIDATE CONSTRAINT orders_customer_fk");
        jdbc.execute("ALTER TABLE orders VALIDATE CONSTRAINT orders_amount_check");
      }
      jdbc.update(
          "UPDATE migration_state SET validation=? WHERE id=1",
          result.matches()
              ? "Passed: counts + SHA-256 + constraints"
              : "FAILED: source and target differ");
      for (var table : result.tables()) {
        log(
            "Validation "
                + (table.matches() ? "passed" : "FAILED")
                + ": "
                + table.table().sqlName()
                + " · source "
                + table.source().count()
                + " / target "
                + table.target().count()
                + " · SHA-256 "
                + (table.source().sha256().equals(table.target().sha256())
                    ? "matches"
                    : "DIFFERS"));
      }
    }

    @Override
    public void complete() {
      jdbc.update(
          "UPDATE migration_state SET stage='COMPLETE',status='COMPLETE',primary_db='POSTGRESQL',checkpoint=now() WHERE id=1");
      log("Cutover complete. All routed writes now go to PostgreSQL; SQL Server remains fenced.");
    }

    @Override
    public void traffic(boolean enabled) {
      jdbc.update("UPDATE migration_state SET traffic=? WHERE id=1", enabled);
      jdbc.update(
          "UPDATE traffic_metrics SET consecutive_errors=0,ops_per_second=0,window_started=now(),window_ops=inserts+updates+deletes+reads WHERE id=1");
      log("Traffic " + (enabled ? "started" : "stopped"));
    }

    @Override
    public long trafficStep() {
      return Objects.requireNonNull(
          jdbc.queryForObject(
              "UPDATE migration_state SET traffic_step=traffic_step+1 WHERE id=1 RETURNING traffic_step-1",
              Long.class));
    }

    @Override
    public TrafficOperationOutcome writeTraffic(TrafficOperation operation) {
      return switch (operation) {
        case INSERT -> {
          long customerId =
              jdbc.queryForObject(
                  "INSERT INTO customers(name,email,active) VALUES('Live customer · عميل',NULL,TRUE) RETURNING id",
                  Long.class);
          long orderId =
              jdbc.queryForObject(
                  "INSERT INTO orders(customer_id,amount,status) VALUES(?,12.3456,'NEW') RETURNING id",
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
              "UPDATE orders SET amount=amount+1.0001,status='UPDATED' WHERE id=?", orderId);
          jdbc.update("UPDATE customers SET active=NOT active WHERE id=?", customerId);
          yield new TrafficOperationOutcome(
              Table.ORDERS, orderId, "status=UPDATED · customer #" + customerId + " toggled");
        }
        case DELETE -> {
          var customer = randomRow("customers", "id");
          if (customer.isEmpty())
            yield new TrafficOperationOutcome(Table.CUSTOMERS, null, "no rows available");
          long customerId = id(customer.get(), "id");
          int orders = jdbc.update("DELETE FROM orders WHERE customer_id=?", customerId);
          jdbc.update("DELETE FROM customers WHERE id=?", customerId);
          yield new TrafficOperationOutcome(
              Table.CUSTOMERS, customerId, orders + " related orders deleted");
        }
        case READ -> {
          var order = randomRow("orders", "id");
          if (order.isEmpty())
            yield new TrafficOperationOutcome(Table.ORDERS, null, "no rows available");
          long orderId = id(order.get(), "id");
          var row = jdbc.queryForMap("SELECT amount,status FROM orders WHERE id=?", orderId);
          yield new TrafficOperationOutcome(
              Table.ORDERS,
              orderId,
              "status=" + row.get("status") + " · amount=" + row.get("amount"));
        }
      };
    }

    private long id(Map<String, Object> row, String column) {
      return ((Number) row.get(column)).longValue();
    }

    /** Seeks the first key at a uniformly random point of the id range; no full scan. */
    private Optional<Map<String, Object>> randomRow(String table, String columns) {
      return jdbc
          .queryForList(
              "SELECT "
                  + columns
                  + " FROM "
                  + table
                  + " WHERE id>=(SELECT MIN(id)+floor(random()*(MAX(id)-MIN(id)+1))::bigint FROM "
                  + table
                  + ") ORDER BY id LIMIT 1")
          .stream()
          .findFirst();
    }

    @Override
    public void recordTraffic(TrafficOperationResult result) {
      String counter = result.operation().name().toLowerCase(Locale.ROOT) + "s";
      String routed = result.target() == Primary.SQL_SERVER ? "sql_server_ops" : "postgres_ops";
      // All right-hand values are pre-update; the rate is recomputed about once per second.
      jdbc.update(
          "UPDATE traffic_metrics SET "
              + counter
              + "="
              + counter
              + "+1,"
              + routed
              + "="
              + routed
              + "+1,consecutive_errors=0,"
              + "ops_per_second=CASE WHEN now()-window_started>=interval '1 second'"
              + " THEN (inserts+updates+deletes+reads+1-window_ops)/extract(epoch FROM now()-window_started)"
              + " ELSE ops_per_second END,"
              + "window_ops=CASE WHEN now()-window_started>=interval '1 second'"
              + " THEN inserts+updates+deletes+reads+1 ELSE window_ops END,"
              + "window_started=CASE WHEN now()-window_started>=interval '1 second'"
              + " THEN now() ELSE window_started END WHERE id=1");
      logTraffic(result);
    }

    @Override
    public int trafficError(TrafficOperationResult result) {
      int failures =
          Objects.requireNonNull(
              jdbc.queryForObject(
                  "UPDATE traffic_metrics SET errors=errors+1,consecutive_errors=consecutive_errors+1 WHERE id=1 RETURNING consecutive_errors",
                  Integer.class));
      logTraffic(result);
      return failures;
    }

    private void logTraffic(TrafficOperationResult result) {
      String database = result.target() == Primary.SQL_SERVER ? "SQL Server" : "PostgreSQL";
      String record =
          result.recordId() == null
              ? result.table().sqlName() + " #—"
              : result.table().sqlName() + " #" + result.recordId();
      log(
          result.operation()
              + " → "
              + database
              + " → "
              + record
              + (result.details().isBlank() ? "" : " → " + result.details())
              + (result.success() ? " ✓" : " ✗"));
    }

    @Override
    public void reset() {
      jdbc.execute("ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_customer_fk");
      jdbc.execute("ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_amount_check");
      jdbc.execute("DROP INDEX IF EXISTS orders_customer_idx");
      jdbc.execute("TRUNCATE orders,customers RESTART IDENTITY");
      // Keep the singleton row: deleting/reinserting it can strand waiting row-lock readers.
      jdbc.update(
          "UPDATE migration_state SET stage=DEFAULT,status=DEFAULT,primary_db=DEFAULT,current_table=DEFAULT,last_id=DEFAULT,customer_bound=DEFAULT,order_bound=DEFAULT,version=DEFAULT,copied=DEFAULT,expected=DEFAULT,batches=DEFAULT,applied=DEFAULT,traffic=DEFAULT,crash_requested=DEFAULT,traffic_step=DEFAULT,validation=DEFAULT,error=DEFAULT,checkpoint=DEFAULT,rows_per_second=DEFAULT,cdc_paused=DEFAULT WHERE id=1");
      jdbc.update(
          "UPDATE traffic_metrics SET inserts=DEFAULT,updates=DEFAULT,deletes=DEFAULT,reads=DEFAULT,errors=DEFAULT,consecutive_errors=DEFAULT,sql_server_ops=DEFAULT,postgres_ops=DEFAULT,window_started=DEFAULT,window_ops=DEFAULT,ops_per_second=DEFAULT WHERE id=1");
      jdbc.update("DELETE FROM replay_receipt");
      jdbc.update("DELETE FROM migration_log");
      log("Reset complete");
    }

    @Override
    public void log(String message) {
      jdbc.update("INSERT INTO migration_log(message) VALUES(?)", message);
      jdbc.update(
          "DELETE FROM migration_log WHERE id<=(SELECT COALESCE(MAX(id),0)-? FROM migration_log)",
          MAX_RETAINED_LOGS);
    }
  }
}
