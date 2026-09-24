package io.zeroshift.integration;

import static org.assertj.core.api.Assertions.*;

import io.zeroshift.application.ValidationService;
import io.zeroshift.domain.*;
import io.zeroshift.infrastructure.PostgresMigrationStore;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.MethodName.class)
class RollbackIT extends DatabaseIntegrationFixture {
  private long newCustomer;
  private long newOrder;

  @Test
  void postCutoverInsertUpdateDeleteRollBackWithSynchronizedIdentities() {
    migrated();
    writeAfterCutover();
    long pgMaxCustomer = pg.queryForObject("SELECT MAX(id) FROM customers", Long.class);
    assertThat(store.reversePending()).isPositive();

    rollBack();

    assertThat(store.state().primary()).isEqualTo(Primary.SQL_SERVER);
    assertThat(store.state().status()).isEqualTo(RunStatus.SUCCESS);
    assertThat(store.reversePending()).isZero();
    assertThat(store.rollback().validation()).startsWith("Passed (final)");
    assertThat(identical()).isTrue();
    assertThat(
            sql.queryForMap("SELECT name,email,active FROM dbo.customers WHERE id=?", newCustomer))
        .containsEntry("name", "Post-cutover · عميل")
        .containsEntry("email", null)
        .containsEntry("active", false);
    assertThat(
            sql.queryForObject(
                "SELECT amount FROM dbo.orders WHERE id=?", BigDecimal.class, newOrder))
        .isEqualByComparingTo("0.0001");
    assertThat(sql.queryForObject("SELECT status FROM dbo.orders WHERE id=3", String.class))
        .isEqualTo("SHIPPED");
    assertThat(sql.queryForObject("SELECT COUNT(*) FROM dbo.customers WHERE id=5", Long.class))
        .isZero();

    // PostgreSQL is the fenced secondary; SQL Server takes writes with identities past every key.
    assertThatThrownBy(() -> pg.update("UPDATE customers SET name='late' WHERE id=1"))
        .hasMessageContaining("frozen");
    var inserted = source.writeTraffic(TrafficOperation.INSERT);
    assertThat(inserted.recordId())
        .isGreaterThan(pgMaxCustomer)
        .isGreaterThan(store.issuedKey(Table.CUSTOMERS));
    long sqlServerOps = traffic.metrics().sqlServerOperations();
    traffic.tick();
    assertThat(traffic.metrics().sqlServerOperations()).isGreaterThan(sqlServerOps);
  }

  @Test
  void rollbackUnderLiveTrafficKeepsEveryPostgresWrite() throws Exception {
    migrated();
    long postgresOps = traffic.metrics().postgresOperations();
    var executor = Executors.newSingleThreadScheduledExecutor();
    executor.scheduleWithFixedDelay(traffic::tick, 0, 15, TimeUnit.MILLISECONDS);
    try {
      Thread.sleep(300);
      rollback.request();
      for (int i = 0; i < 40 && store.state().stage() != Stage.ROLLED_BACK; i++) {
        // Writes are already held behind the fence here; stopping traffic now keeps SQL Server
        // free of post-switch writes so the two databases can be compared exactly afterwards.
        if (store.state().stage() == Stage.FINAL_SYNC) traffic.toggle(false);
        coordinator.tick();
        assertThat(store.state().error()).isEmpty();
        Thread.sleep(40);
      }
    } finally {
      executor.shutdown();
      assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }
    assertThat(store.state().stage()).isEqualTo(Stage.ROLLED_BACK);
    assertThat(traffic.metrics().postgresOperations()).isGreaterThan(postgresOps + 5);
    assertThat(traffic.metrics().errors()).isZero();
    assertThat(identical()).isTrue();
  }

  @Test
  void crashBetweenSqlServerCommitAndAcknowledgementReplaysIdempotently() {
    migrated();
    // Small enough that the first reverse page (customers) holds it all.
    newCustomer =
        pg.queryForObject(
            "INSERT INTO customers(name,email,active) VALUES('Crash probe',NULL,TRUE) RETURNING id",
            Long.class);
    pg.update("INSERT INTO orders(customer_id,amount,status) VALUES(?,1,'NEW')", newCustomer);
    long pending = store.reversePending();
    assertThat(pending).isEqualTo(2);
    rollback.request();
    coordinator.tick();
    assertThat(store.state().stage()).isEqualTo(Stage.REVERSE_CATCH_UP);

    coordinator.crash();
    coordinator.tick();

    assertThat(store.state().status()).isEqualTo(RunStatus.CRASHED);
    // SQL Server committed the first page; its acknowledgement rolled back with the crash.
    assertThat(
            sql.queryForObject(
                "SELECT COUNT(*) FROM dbo.customers WHERE id=?", Long.class, newCustomer))
        .isOne();
    assertThat(store.reversePending()).isEqualTo(pending);

    coordinator.resume();
    finishRollback();
    assertThat(identical()).isTrue();
    assertThat(store.state().stage()).isEqualTo(Stage.ROLLED_BACK);
  }

  @Test
  void restartResumesFromThePersistedStageAndKeepsTheFence() {
    migrated();
    writeAfterCutover();
    rollback.request();
    for (int i = 0; i < 10 && store.state().stage() != Stage.FINAL_SYNC; i++) coordinator.tick();
    assertThat(store.state().stage()).isEqualTo(Stage.FINAL_SYNC);

    store = new PostgresMigrationStore(targetDataSource);
    wire();
    coordinator.recover();

    assertThat(store.state().status()).isEqualTo(RunStatus.PAUSED);
    assertThat(store.state().stage()).isEqualTo(Stage.FINAL_SYNC);
    assertThat(store.state().primary()).isEqualTo(Primary.POSTGRESQL);
    assertThatThrownBy(() -> pg.update("UPDATE customers SET name='during restart' WHERE id=1"))
        .hasMessageContaining("frozen");
    coordinator.resume();
    finishRollback();
    assertThat(identical()).isTrue();
  }

  @Test
  void outOfBandSqlServerWriteBlocksRollbackAndIsNeverOverwritten() {
    migrated();
    pg.update("UPDATE customers SET name='from PostgreSQL' WHERE id=3");
    writeSqlServerOutsideZeroShift("UPDATE dbo.customers SET name='from SQL Server' WHERE id=3");

    rollback.request();
    coordinator.tick();

    assertThat(store.state().status()).isEqualTo(RunStatus.FAILED);
    assertThat(store.state().error()).contains("customers #3").contains("not overwritten");
    assertThat(store.rollback().conflicts()).isOne();
    assertThat(sqlName(3)).isEqualTo("from SQL Server");
    assertThat(store.state().primary()).isEqualTo(Primary.POSTGRESQL);

    rollback.abort();
    assertThat(store.state().stage()).isEqualTo(Stage.COMPLETED);
    pg.update("UPDATE customers SET name='writable again' WHERE id=4");
    assertThat(store.reversePending()).isEqualTo(2);
  }

  @Test
  void conflictArrivingDuringReverseCatchUpIsCaughtUnderTheKeyLock() {
    migrated();
    pg.update("UPDATE customers SET name='from PostgreSQL' WHERE id=3");
    rollback.request();
    coordinator.tick();
    assertThat(store.state().stage()).isEqualTo(Stage.REVERSE_CATCH_UP);

    writeSqlServerOutsideZeroShift(
        "UPDATE dbo.customers SET name='late SQL Server write' WHERE id=3");
    coordinator.tick();

    assertThat(store.state().status()).isEqualTo(RunStatus.FAILED);
    assertThat(sqlName(3)).isEqualTo("late SQL Server write");
    assertThat(store.reversePending()).isOne();
  }

  @Test
  void writeThatEscapedCaptureFailsValidationBeforeAnyFreeze() {
    migrated();
    pg.execute("ALTER TABLE customers DISABLE TRIGGER zeroshift_capture");
    pg.update("UPDATE customers SET name='uncaptured' WHERE id=2");
    pg.execute("ALTER TABLE customers ENABLE TRIGGER zeroshift_capture");

    rollback.request();
    for (int i = 0; i < 5 && store.state().active(); i++) coordinator.tick();

    assertThat(store.state().status()).isEqualTo(RunStatus.FAILED);
    assertThat(store.state().stage()).isEqualTo(Stage.ROLLBACK_VALIDATION);
    assertThat(store.rollback().validation()).startsWith("FAILED (settled rows)");
    assertThat(store.state().primary()).isEqualTo(Primary.POSTGRESQL);
    pg.update("UPDATE customers SET name='still writable' WHERE id=4");
  }

  @Test
  void finalValidationFailureKeepsPostgresPrimaryAndFencedUntilAborted() {
    migrated();
    writeAfterCutover();
    rollback.request();
    for (int i = 0; i < 10 && store.state().stage() != Stage.FINAL_SYNC; i++) coordinator.tick();
    // A faulty replay: written through reverse sync, so it is not a conflict, only a difference.
    writeback.apply(
        Table.CUSTOMERS,
        List.of(
            new Change(1, Change.Operation.UPDATE, new Row.Customer(1, "tampered", null, true), 0)),
        store.rollback().baselineVersion());

    coordinator.tick();

    assertThat(store.state().status()).isEqualTo(RunStatus.FAILED);
    assertThat(store.state().stage()).isEqualTo(Stage.FINAL_SYNC);
    assertThat(store.rollback().validation()).startsWith("FAILED (final)");
    assertThat(store.state().primary()).isEqualTo(Primary.POSTGRESQL);
    assertThatThrownBy(() -> pg.update("UPDATE customers SET name='x' WHERE id=4"))
        .hasMessageContaining("frozen");
    assertThatThrownBy(() -> source.writeTraffic(TrafficOperation.INSERT))
        .hasMessageContaining("frozen");

    rollback.abort();
    assertThat(store.state().stage()).isEqualTo(Stage.COMPLETED);
    assertThat(store.state().successful()).isTrue();
    long postgresOps = traffic.metrics().postgresOperations();
    traffic.tick();
    assertThat(traffic.metrics().postgresOperations()).isGreaterThan(postgresOps);
  }

  @Test
  void rollbackNeedsASuccessfulCutoverWithCaptureActiveSinceThen() {
    source.seed(20);
    ready();
    assertThatThrownBy(rollback::request)
        .isInstanceOf(InvalidAction.class)
        .hasMessageContaining("successful cutover");

    completeCutover();
    pg.update("UPDATE rollback_state SET capture_since=NULL");
    assertThatThrownBy(rollback::request)
        .isInstanceOf(InvalidAction.class)
        .hasMessageContaining("Reverse capture was not active");
    assertThat(store.state().stage()).isEqualTo(Stage.COMPLETED);
  }

  private void migrated() {
    source.seed(20);
    ready();
    traffic.toggle(true);
    completeCutover();
    assertThat(store.state().primary()).isEqualTo(Primary.POSTGRESQL);
  }

  /** Real PostgreSQL writes after cutover: routed traffic plus explicit edge cases. */
  private void writeAfterCutover() {
    for (int i = 0; i < 8; i++) traffic.tick();
    newCustomer =
        pg.queryForObject(
            "INSERT INTO customers(name,email,active) VALUES('Post-cutover · عميل',NULL,FALSE) RETURNING id",
            Long.class);
    newOrder =
        pg.queryForObject(
            "INSERT INTO orders(customer_id,amount,status) VALUES(?,0.0001,'NEW') RETURNING id",
            Long.class,
            newCustomer);
    pg.update("UPDATE orders SET status='SHIPPED' WHERE id=3");
    pg.update("DELETE FROM orders WHERE customer_id=5");
    pg.update("DELETE FROM customers WHERE id=5");
  }

  private void rollBack() {
    rollback.request();
    finishRollback();
  }

  private void finishRollback() {
    for (int i = 0; i < 10 && store.state().stage() != Stage.ROLLED_BACK; i++) {
      coordinator.tick();
      assertThat(store.state().error()).isEmpty();
    }
    assertThat(store.state().stage()).isEqualTo(Stage.ROLLED_BACK);
  }

  private boolean identical() {
    return new ValidationService(source, store, 7).validate().matches();
  }

  private void writeSqlServerOutsideZeroShift(String statement) {
    sql.update("UPDATE dbo.migration_gate SET frozen=0 WHERE id=1");
    try {
      sql.update(statement);
    } finally {
      sql.update("UPDATE dbo.migration_gate SET frozen=1 WHERE id=1");
    }
  }

  private String sqlName(long id) {
    return sql.queryForObject("SELECT name FROM dbo.customers WHERE id=?", String.class, id);
  }
}
