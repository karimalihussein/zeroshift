package io.zeroshift.integration;

import static org.assertj.core.api.Assertions.*;

import io.zeroshift.application.*;
import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import io.zeroshift.infrastructure.*;
import java.math.BigDecimal;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.*;

@Testcontainers
@TestMethodOrder(MethodOrderer.MethodName.class)
class MigrationIT extends DatabaseIntegrationFixture {
  @Test
  void bulkCopyPreservesUnicodeNullEmptyQuotesAndDecimalPrecision() {
    source.seed(20);
    sql.update("UPDATE dbo.customers SET name=?,email=? WHERE id=1", "عميل\ncomma,\"quote\"", "");
    sql.update("UPDATE dbo.orders SET amount=123456789012345.6789 WHERE id=1");
    ready();
    assertThat(cutover.inspect().matches()).isTrue();
    assertThat(pg.queryForObject("SELECT email FROM customers WHERE id=1", String.class)).isEmpty();
    assertThat(pg.queryForObject("SELECT amount FROM orders WHERE id=1", BigDecimal.class))
        .isEqualByComparingTo("123456789012345.6789");
  }

  @Test
  void capturesInsertUpdateDeleteAndForeignKeyTransactions() {
    source.seed(20);
    ready();
    source.writeTraffic(TrafficOperation.forStep(0));
    source.writeTraffic(TrafficOperation.forStep(1));
    source.writeTraffic(TrafficOperation.forStep(2));
    coordinator.tick();
    assertThat(cutover.inspect().matches()).isTrue();
    assertThat(store.state().applied()).isGreaterThanOrEqualTo(4);
    assertThat(pg.queryForObject("SELECT count(*) FROM customers WHERE id=1", Long.class)).isZero();
    assertThat(
            pg.queryForObject(
                "SELECT status FROM orders WHERE id=(SELECT MAX(id) FROM orders)", String.class))
        .isEqualTo("UPDATED");
  }

  @Test
  void checkpointsSurviveNewBackendObjectsAndSimulatedCrashRollsBack() {
    source.seed(30);
    coordinator.start();
    coordinator.tick();
    long copied = store.state().copied(), key = store.state().lastId();
    coordinator.crash();
    coordinator.tick();
    assertThat(store.state().status()).isEqualTo(RunStatus.CRASHED);
    assertThat(store.state().copied()).isEqualTo(copied);
    assertThat(store.state().lastId()).isEqualTo(key);
    assertThat(store.count(Table.CUSTOMERS)).isEqualTo(copied);
    store = new PostgresMigrationStore(targetDataSource);
    source = new SqlServerReader(sourceDataSource);
    wire();
    coordinator.recover();
    coordinator.resume();
    while (store.state().stage() != Stage.READY) {
      coordinator.tick();
      assertThat(store.state().status()).isEqualTo(RunStatus.RUNNING);
    }
    assertThat(cutover.inspect().matches()).isTrue();
  }

  @Test
  void concurrentWritesDuringSnapshotAndCatchupConverge() throws Exception {
    source.seed(100);
    coordinator.start();
    try (var executor = Executors.newSingleThreadExecutor()) {
      Future<?> writes =
          executor.submit(
              () -> {
                for (int i = 0; i < 60; i++) source.writeTraffic(TrafficOperation.forStep(i));
              });
      while (store.state().stage() != Stage.READY) {
        coordinator.tick();
        assertThat(store.state().status()).isEqualTo(RunStatus.RUNNING);
      }
      writes.get(60, TimeUnit.SECONDS);
    }
    coordinator.tick();
    assertThat(cutover.inspect().matches()).isTrue();
  }

  @Test
  void cutoverFencesSourceAndRoutesNewWritesWithSynchronizedSequences() {
    source.seed(20);
    ready();
    traffic.toggle(true);
    traffic.tick();
    coordinator.tick();
    cutover.request();
    coordinator.tick();
    assertThat(store.state().primary()).isEqualTo(Primary.POSTGRESQL);
    long sourceCount = source.count(Table.CUSTOMERS);
    traffic.tick();
    traffic.tick();
    traffic.tick();
    assertThat(source.count(Table.CUSTOMERS)).isEqualTo(sourceCount);
    assertThat(pg.queryForObject("SELECT MAX(id) FROM customers", Long.class))
        .isGreaterThan(sql.queryForObject("SELECT MAX(id) FROM dbo.customers", Long.class));
    assertThatThrownBy(() -> source.writeTraffic(TrafficOperation.forStep(0)))
        .hasMessageContaining("frozen");
    assertThat(store.state().traffic()).isTrue();
  }

  @Test
  void finalDrainFlushesDeferredForeignKeyEventsBeforeValidation() {
    source.seed(20);
    ready();
    source.writeTraffic(TrafficOperation.INSERT);
    assertThat(cutover.inspect().matches()).isTrue();
    source.writeTraffic(TrafficOperation.DELETE);
    cutover.request();
    coordinator.tick();
    assertThat(store.state().error()).isEmpty();
    assertThat(store.state().stage()).isEqualTo(Stage.COMPLETE);
  }

  @Test
  void captureWindowExcludesLaterCommitsAcrossAllPages() {
    source.seed(3);
    long baseline = source.boundary().version();
    source.writeTraffic(TrafficOperation.INSERT);
    long high;
    try (var capture = source.capture(baseline)) {
      high = capture.version();
      source.writeTraffic(TrafficOperation.UPDATE);
      source.writeTraffic(TrafficOperation.INSERT);
      var customers = capture.read(Table.CUSTOMERS, 0, 1);
      assertThat(customers).hasSize(1);
      assertThat(customers.getFirst().row())
          .isInstanceOfSatisfying(
              Row.Customer.class, customer -> assertThat(customer.active()).isTrue());
      assertThat(capture.read(Table.CUSTOMERS, customers.getFirst().id(), 1)).isEmpty();
      assertThat(capture.read(Table.ORDERS, 0, 10)).hasSize(1);
    }
    try (var next = source.capture(high)) {
      assertThat(next.read(Table.CUSTOMERS, 0, 10)).hasSize(2);
    }
  }

  @Test
  void commitOrderCannotLoseAnEarlierAllocatedIdentity() throws Exception {
    long baseline = source.boundary().version();
    try (var first = sourceDataSource.getConnection();
        var second = sourceDataSource.getConnection()) {
      first.setAutoCommit(false);
      try (var insert = first.createStatement()) {
        insert.executeUpdate(
            "INSERT dbo.customers(name,email,active) VALUES('commits last',NULL,1)");
      }
      try (var insert = second.createStatement()) {
        insert.executeUpdate(
            "INSERT dbo.customers(name,email,active) VALUES('commits first',NULL,1)");
      }
      long high;
      long laterIdentity;
      try (var capture = source.capture(baseline)) {
        high = capture.version();
        var changes = capture.read(Table.CUSTOMERS, 0, 10);
        assertThat(changes).hasSize(1);
        laterIdentity = changes.getFirst().id();
      }
      first.commit();
      try (var capture = source.capture(high)) {
        var changes = capture.read(Table.CUSTOMERS, 0, 10);
        assertThat(changes).hasSize(1);
        assertThat(changes.getFirst().id()).isLessThan(laterIdentity);
      }
    }
  }

  @Test
  void captureCrashRetainsWatermarkAndReplaysBothTables() {
    source.seed(20);
    ready();
    long version = store.state().version();
    source.writeTraffic(TrafficOperation.INSERT);
    coordinator.crash();
    coordinator.tick();
    assertThat(store.state().status()).isEqualTo(RunStatus.CRASHED);
    assertThat(store.state().version()).isEqualTo(version);
    assertThat(store.count(Table.CUSTOMERS)).isEqualTo(20);
    coordinator.resume();
    coordinator.tick();
    assertThat(cutover.inspect().matches()).isTrue();
  }

  @Test
  void emptyMigrationSequencesStartAtOne() {
    ready();
    cutover.request();
    coordinator.tick();
    traffic.toggle(true);
    traffic.tick();
    assertThat(pg.queryForObject("SELECT MIN(id) FROM customers", Long.class)).isEqualTo(1L);
  }

  @Test
  void failedValidationBlocksCutoverAndRetainsFence() {
    source.seed(20);
    ready();
    pg.update("UPDATE customers SET name='corrupted' WHERE id=1");
    assertThat(cutover.inspect().matches()).isFalse();
    cutover.request();
    coordinator.tick();
    assertThat(store.state().status()).isEqualTo(RunStatus.FAILED);
    assertThat(store.state().primary()).isEqualTo(Primary.SQL_SERVER);
    assertThatThrownBy(() -> source.writeTraffic(TrafficOperation.forStep(0)))
        .hasMessageContaining("frozen");
  }

  @Test
  void freezeStageRecoversAfterRestartBeforeRoutingCommit() {
    source.seed(20);
    ready();
    cutover.request();
    source.freeze(true);
    store = new PostgresMigrationStore(targetDataSource);
    wire();
    coordinator.recover();
    assertThat(store.state().status()).isEqualTo(RunStatus.PAUSED);
    coordinator.resume();
    coordinator.tick();
    assertThat(store.state().stage()).isEqualTo(Stage.COMPLETE);
  }

  @Test
  void pauseStopsCheckpointAdvancementAndInvalidTransitionsAreRejected() {
    source.seed(20);
    coordinator.start();
    coordinator.tick();
    coordinator.pause();
    long key = store.state().lastId();
    coordinator.tick();
    assertThat(store.state().lastId()).isEqualTo(key);
    assertThatThrownBy(coordinator::start).isInstanceOf(InvalidAction.class);
    assertThatThrownBy(cutover::request).isInstanceOf(InvalidAction.class);
    coordinator.resume();
    coordinator.tick();
    assertThat(store.state().lastId()).isGreaterThan(key);
  }

  @Test
  void retentionGapFailsClosed() {
    source.seed(20);
    ready();
    sql.execute("ALTER TABLE dbo.customers DISABLE CHANGE_TRACKING");
    source.writeTraffic(TrafficOperation.forStep(0));
    sql.execute("ALTER TABLE dbo.customers ENABLE CHANGE_TRACKING");
    coordinator.tick();
    assertThat(store.state().status()).isEqualTo(RunStatus.FAILED);
    assertThat(store.state().error()).contains("retention expired");
  }

  @Test
  void targetConstraintFailureRollsBackRowsAndWatermark() {
    source.seed(20);
    ready();
    long version = store.state().version();
    pg.execute("ALTER TABLE customers ADD CONSTRAINT reject_live CHECK(name NOT LIKE 'Live%')");
    try {
      source.writeTraffic(TrafficOperation.forStep(0));
      coordinator.tick();
      assertThat(store.state().status()).isEqualTo(RunStatus.FAILED);
      assertThat(store.state().version()).isEqualTo(version);
    } finally {
      pg.execute("ALTER TABLE customers DROP CONSTRAINT reject_live");
    }
    coordinator.resume();
    coordinator.tick();
    assertThat(cutover.inspect().matches()).isTrue();
  }

  @Test
  void chunkedSeedGeneratesContiguousRowsAcrossChunksAndResetsToIdentityOne() {
    var demo = new DemoDataService(source, store, 20);
    assertThatThrownBy(() -> demo.seed(0)).isInstanceOf(InvalidAction.class);
    assertThatThrownBy(() -> demo.seed(DemoDataService.MAX_ROWS + 1))
        .isInstanceOf(InvalidAction.class);
    demo.seed(100_001);
    assertThat(
            sql.queryForList("SELECT COUNT_BIG(*) n, MIN(id) lo, MAX(id) hi FROM dbo.customers")
                .getFirst())
        .containsEntry("n", 100_001L)
        .containsEntry("lo", 1L)
        .containsEntry("hi", 100_001L);
    assertThat(
            sql.queryForObject(
                "SELECT COUNT_BIG(*) FROM dbo.orders WHERE id=customer_id", Long.class))
        .isEqualTo(100_001L);
    assertThat(sql.queryForObject("SELECT name FROM dbo.customers WHERE id=100001", String.class))
        .isEqualTo("Customer 100001 · عميل");
    assertThat(sql.queryForObject("SELECT email FROM dbo.customers WHERE id=7", String.class))
        .isNull();
    demo.reset();
    demo.seed(null);
    assertThat(sql.queryForObject("SELECT MIN(id) FROM dbo.orders", Long.class)).isEqualTo(1L);
    assertThat(source.count(Table.CUSTOMERS)).isEqualTo(20);
  }
}
