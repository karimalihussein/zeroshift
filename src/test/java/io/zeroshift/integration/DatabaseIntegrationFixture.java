package io.zeroshift.integration;

import static org.assertj.core.api.Assertions.*;

import io.zeroshift.application.*;
import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import io.zeroshift.infrastructure.*;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;

@Testcontainers
abstract class DatabaseIntegrationFixture {
  @Container
  static final MSSQLServerContainer<?> SQL =
      new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2022-latest")
          .acceptLicense()
          .withStartupTimeout(Duration.ofMinutes(4));

  @Container
  static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:17-alpine");

  protected SqlServerReader source;
  protected PostgresMigrationStore store;
  protected MigrationCoordinator coordinator;
  protected CutoverService cutover;
  protected TrafficSimulator traffic;
  protected JdbcTemplate sql;
  protected JdbcTemplate pg;
  protected DriverManagerDataSource targetDataSource;
  protected DriverManagerDataSource sourceDataSource;

  @BeforeEach
  void setup() {
    var master =
        new JdbcTemplate(
            new DriverManagerDataSource(SQL.getJdbcUrl(), SQL.getUsername(), SQL.getPassword()));
    master.execute("IF DB_ID('zeroshift_java') IS NULL CREATE DATABASE zeroshift_java");
    master.execute("ALTER DATABASE zeroshift_java SET ALLOW_SNAPSHOT_ISOLATION ON");
    master.execute(
        "IF NOT EXISTS(SELECT 1 FROM sys.change_tracking_databases WHERE database_id=DB_ID('zeroshift_java')) ALTER DATABASE zeroshift_java SET CHANGE_TRACKING=ON (CHANGE_RETENTION=7 DAYS,AUTO_CLEANUP=ON)");
    sourceDataSource =
        new DriverManagerDataSource(
            SQL.getJdbcUrl() + ";databaseName=zeroshift_java",
            SQL.getUsername(),
            SQL.getPassword());
    targetDataSource =
        new DriverManagerDataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    new SchemaInitializer(targetDataSource, sourceDataSource);
    source = new SqlServerReader(sourceDataSource);
    store = new PostgresMigrationStore(targetDataSource);
    sql = new JdbcTemplate(sourceDataSource);
    pg = new JdbcTemplate(targetDataSource);
    new DemoDataService(source, store, 20).reset();
    wire();
  }

  protected void wire() {
    var changes = new ChangeCatchUp(source, 7);
    cutover = new CutoverService(source, store, changes, new ValidationService(source, store, 7));
    coordinator =
        new MigrationCoordinator(store, source, new SnapshotBatch(source, 7), changes, cutover);
    traffic = new TrafficSimulator(store, source);
  }

  protected void ready() {
    coordinator.start();
    for (int i = 0; i < 100 && store.state().stage() != Stage.READY; i++) {
      coordinator.tick();
      assertThat(store.state().error()).isEmpty();
    }
    assertThat(store.state().stage()).isEqualTo(Stage.READY);
  }
}
