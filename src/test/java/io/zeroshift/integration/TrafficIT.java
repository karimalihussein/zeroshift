package io.zeroshift.integration;

import static org.assertj.core.api.Assertions.*;

import io.zeroshift.domain.*;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Predicate;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.*;

@Testcontainers
class TrafficIT extends DatabaseIntegrationFixture {
  @Test
  void trafficCommitsRealRowChangesAndCountsEveryOperation() {
    source.seed(20);
    traffic.toggle(true);
    for (int i = 0; i < 10; i++) traffic.tick(); // one full mix
    var metrics = traffic.metrics();
    assertThat(metrics.total()).isEqualTo(10);
    assertThat(metrics.reads()).isEqualTo(4);
    assertThat(metrics.updates()).isEqualTo(3);
    assertThat(metrics.inserts()).isEqualTo(2);
    assertThat(metrics.deletes()).isEqualTo(1);
    assertThat(metrics.errors()).as(store.logs().toString()).isZero();
    assertThat(metrics.sqlServerOperations()).isEqualTo(10);
    assertThat(metrics.target()).isEqualTo(Primary.SQL_SERVER);
    assertThat(source.count(Table.CUSTOMERS)).isEqualTo(20 + 2 - 1);
    assertThat(source.count(Table.ORDERS)).isEqualTo(20 + 2 - 1);
    // The mix ends with an UPDATE after its DELETE, so at least one updated row survives.
    assertThat(
            sql.queryForObject(
                "SELECT COUNT(*) FROM dbo.orders WHERE status='UPDATED'", Long.class))
        .isPositive();
    assertThat(store.count(Table.CUSTOMERS)).as("no migration: PostgreSQL untouched").isZero();
    assertThat(store.logs())
        .filteredOn(log -> log.contains(" → SQL Server → "))
        .hasSize(10)
        .anySatisfy(
            log ->
                assertThat(log)
                    .contains("INSERT → SQL Server → customers #")
                    .contains("order #")
                    .endsWith("✓"))
        .anySatisfy(
            log ->
                assertThat(log)
                    .contains("UPDATE → SQL Server → orders #")
                    .contains("status=UPDATED")
                    .endsWith("✓"))
        .anySatisfy(
            log ->
                assertThat(log)
                    .contains("DELETE → SQL Server → customers #")
                    .contains("related orders deleted")
                    .endsWith("✓"));
  }

  @Test
  void trafficRunsThroughSnapshotAndCdcReplaysEveryChangeItMade() throws Exception {
    source.seed(200);
    traffic.toggle(true);
    coordinator.start();
    long bound = store.state().customerBound();
    long duringSnapshot;
    try (var _ = new BackgroundTraffic()) {
      long before = traffic.metrics().total();
      while (store.state().stage() == Stage.SNAPSHOT) {
        // Interleave strictly: a new committed operation between every snapshot batch.
        long seen = traffic.metrics().total();
        await(m -> m.total() > seen);
        coordinator.tick();
        assertThat(store.state().status()).isEqualTo(RunStatus.RUNNING);
      }
      duringSnapshot = traffic.metrics().total() - before;
      while (store.state().stage() != Stage.READY) coordinator.tick();
    }
    coordinator.tick(); // READY drains whatever committed after the last catch-up
    assertThat(duringSnapshot).isGreaterThanOrEqualTo(store.state().batches());
    assertThat(traffic.metrics().errors()).as(store.logs().toString()).isZero();
    assertThat(store.state().applied()).isPositive();
    long insertedDuringMigration =
        sql.queryForObject("SELECT COUNT(*) FROM dbo.customers WHERE id>?", Long.class, bound);
    assertThat(insertedDuringMigration).isPositive();
    assertThat(pg.queryForObject("SELECT COUNT(*) FROM customers WHERE id>?", Long.class, bound))
        .as("rows inserted after the snapshot boundary reach PostgreSQL only through CDC")
        .isEqualTo(insertedDuringMigration);
    assertThat(cutover.inspect().matches()).isTrue();
  }

  @Test
  void trafficKeepsRunningAcrossCutoverAndThenWritesOnlyToPostgres() throws Exception {
    source.seed(50);
    ready();
    traffic.toggle(true);
    long sqlServerOps, sourceVersion, sourceCustomers;
    try (var _ = new BackgroundTraffic()) {
      await(m -> m.sqlServerOperations() >= 10);
      cutover.request();
      coordinator.tick();
      assertThat(store.state().error()).isEmpty();
      assertThat(store.state().stage()).isEqualTo(Stage.COMPLETE);
      sqlServerOps = traffic.metrics().sqlServerOperations();
      sourceVersion = changeTrackingVersion();
      sourceCustomers = source.count(Table.CUSTOMERS);
      await(m -> m.postgresOperations() >= 20); // no restart: the same loop keeps going
    }
    var metrics = traffic.metrics();
    assertThat(metrics.running()).isTrue();
    assertThat(metrics.target()).isEqualTo(Primary.POSTGRESQL);
    assertThat(metrics.errors()).as(store.logs().toString()).isZero();
    assertThat(metrics.sqlServerOperations()).isEqualTo(sqlServerOps);
    assertThat(changeTrackingVersion()).as("no commit reached SQL Server").isEqualTo(sourceVersion);
    assertThat(source.count(Table.CUSTOMERS)).isEqualTo(sourceCustomers);
    assertThat(pg.queryForObject("SELECT MAX(id) FROM customers", Long.class))
        .isGreaterThan(sql.queryForObject("SELECT MAX(id) FROM dbo.customers", Long.class));
    assertThat(store.logs())
        .anySatisfy(
            log ->
                assertThat(log)
                    .contains("→ PostgreSQL →")
                    .matches("^\\[\\d{2}:\\d{2}:\\d{2}] .+ [✓✗]$"));
  }

  private long changeTrackingVersion() {
    return sql.queryForObject("SELECT CHANGE_TRACKING_CURRENT_VERSION()", Long.class);
  }

  private void await(Predicate<TrafficMetrics> condition) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (!condition.test(traffic.metrics())) {
      assertThat(System.nanoTime())
          .as("traffic stalled: " + traffic.metrics())
          .isLessThan(deadline);
      Thread.sleep(25);
    }
  }

  /** Runs the real simulator on its own thread, as the scheduled worker does in the app. */
  private final class BackgroundTraffic implements AutoCloseable {
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    BackgroundTraffic() {
      executor.scheduleWithFixedDelay(traffic::tick, 0, 20, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws InterruptedException {
      executor.shutdown();
      assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }
  }
}
