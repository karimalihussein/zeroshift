package db.platform;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

/**
 * Creates Debezium's replication slot with the schema, not when the connector first starts. From
 * this moment PostgreSQL retains every outbox insert, so orders placed while Kafka Connect is down
 * (or not deployed yet) are still published once it starts. A Java migration because PostgreSQL
 * refuses to create a slot inside a transaction that has written anything.
 *
 * <p>A slot nobody reads retains WAL forever, so a service without an outbox connector opts out
 * with {@code spring.flyway.placeholders.outboxSlot=false}.
 */
public class V0_2__OutboxReplicationSlot extends BaseJavaMigration {
  @Override
  public boolean canExecuteInTransaction() {
    return false;
  }

  @Override
  public void migrate(Context context) throws Exception {
    if ("false".equals(context.getConfiguration().getPlaceholders().get("outboxSlot"))) return;
    try (var statement = context.getConnection().createStatement()) {
      statement.execute(
          "SELECT pg_create_logical_replication_slot(current_database() || '_outbox', 'pgoutput')"
              + " WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots"
              + " WHERE slot_name = current_database() || '_outbox')");
    }
  }
}
