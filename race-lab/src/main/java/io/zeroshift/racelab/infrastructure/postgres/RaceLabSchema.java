package io.zeroshift.racelab.infrastructure.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;

/** The lab's connection pool and schema: race_lab, migrated by its own Flyway history. */
public final class RaceLabSchema {
  public static final String SCHEMA = "race_lab";

  private RaceLabSchema() {}

  /**
   * A pool of its own, so up to 20 racing transactions and the lock monitor never take connections
   * from the control plane. Every connection's search_path is race_lab.
   */
  public static HikariDataSource pool(String url, String user, String password) {
    var config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setUsername(user);
    config.setPassword(password);
    config.setSchema(SCHEMA);
    config.setPoolName("race-lab");
    config.setMaximumPoolSize(28);
    config.setMinimumIdle(0);
    config.setIdleTimeout(60_000);
    config.setConnectionTimeout(10_000);
    return new HikariDataSource(config);
  }

  public static void migrate(javax.sql.DataSource dataSource) {
    Flyway.configure()
        .dataSource(dataSource)
        .schemas(SCHEMA)
        .defaultSchema(SCHEMA)
        .createSchemas(true)
        .table("flyway_race_lab_history")
        .locations("classpath:db/race-lab")
        .load()
        .migrate();
  }
}
