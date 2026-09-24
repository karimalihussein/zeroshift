package io.zeroshift.infrastructure;

import io.zeroshift.domain.*;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.postgresql.PGConnection;

/** COPY into a transaction-local staging table, then merge. Replays are idempotent. */
final class PostgresBulkLoader {
  void upsert(Connection connection, Table table, List<Row> rows) {
    if (rows.isEmpty()) return;
    String name = table.sqlName();
    try (var statement = connection.createStatement()) {
      statement.execute(
          "CREATE TEMP TABLE IF NOT EXISTS stage_" + name + " (LIKE " + name + ") ON COMMIT DROP");
      statement.execute("TRUNCATE stage_" + name);
      StringBuilder csv = new StringBuilder();
      for (Row row : rows) {
        switch (row) {
          case Row.Customer c ->
              csv.append(c.id())
                  .append(',')
                  .append(field(c.name()))
                  .append(',')
                  .append(field(c.email()))
                  .append(',')
                  .append(c.active());
          case Row.Order o ->
              csv.append(o.id())
                  .append(',')
                  .append(o.customerId())
                  .append(',')
                  .append(o.amount().toPlainString())
                  .append(',')
                  .append(field(o.status()));
        }
        csv.append('\n');
      }
      connection
          .unwrap(PGConnection.class)
          .getCopyAPI()
          .copyIn(
              "COPY stage_" + name + " (" + Rows.columns(table) + ") FROM STDIN WITH (FORMAT csv)",
              new StringReader(csv.toString()));
      String update =
          table == Table.CUSTOMERS
              ? "name=EXCLUDED.name,email=EXCLUDED.email,active=EXCLUDED.active"
              : "customer_id=EXCLUDED.customer_id,amount=EXCLUDED.amount,status=EXCLUDED.status";
      statement.executeUpdate(
          "INSERT INTO "
              + name
              + " SELECT * FROM stage_"
              + name
              + " ON CONFLICT(id) DO UPDATE SET "
              + update);
    } catch (SQLException | IOException e) {
      throw new MigrationException("COPY failed for " + name, e);
    }
  }

  static String field(String value) {
    return value == null ? "" : "\"" + value.replace("\"", "\"\"") + "\"";
  }
}
