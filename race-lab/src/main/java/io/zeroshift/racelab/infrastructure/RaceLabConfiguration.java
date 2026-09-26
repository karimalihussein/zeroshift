package io.zeroshift.racelab.infrastructure;

import com.zaxxer.hikari.HikariDataSource;
import io.zeroshift.racelab.application.ExperimentEngine;
import io.zeroshift.racelab.application.Experiments;
import io.zeroshift.racelab.application.RunComparison;
import io.zeroshift.racelab.application.RunStreams;
import io.zeroshift.racelab.application.port.LabDatabase;
import io.zeroshift.racelab.application.port.RunRepository;
import io.zeroshift.racelab.application.port.Tracing;
import io.zeroshift.racelab.experiments.Catalog;
import io.zeroshift.racelab.infrastructure.postgres.PostgresLabDatabase;
import io.zeroshift.racelab.infrastructure.postgres.PostgresRunRepository;
import io.zeroshift.racelab.infrastructure.postgres.RaceLabSchema;
import io.zeroshift.racelab.web.RaceLabSettings;
import io.zeroshift.racelab.web.RunEventStream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the race lab into the control plane: its own pool on the control plane's PostgreSQL (schema
 * race_lab, migrated at startup), the engine, and the web pieces.
 */
@Configuration
@EnableConfigurationProperties(RaceLabSettings.class)
public class RaceLabConfiguration {
  @Bean(name = "raceLabDataSource", destroyMethod = "close")
  HikariDataSource raceLabDataSource(
      @Value("${spring.datasource.url}") String url,
      @Value("${spring.datasource.username}") String user,
      @Value("${spring.datasource.password}") String password) {
    var pool = RaceLabSchema.pool(url, user, password);
    RaceLabSchema.migrate(pool);
    return pool;
  }

  @Bean
  LabDatabase raceLabDatabase(@Qualifier("raceLabDataSource") HikariDataSource pool) {
    return new PostgresLabDatabase(pool);
  }

  @Bean
  RunRepository raceLabRuns(@Qualifier("raceLabDataSource") HikariDataSource pool) {
    return new PostgresRunRepository(pool);
  }

  @Bean
  Tracing raceLabTracing() {
    return new OtelTracing();
  }

  @Bean
  RunStreams raceLabStreams() {
    return new RunStreams();
  }

  @Bean(destroyMethod = "close")
  ExperimentEngine raceLabEngine(
      LabDatabase db, RunRepository runs, Tracing tracing, RunStreams streams) {
    return new ExperimentEngine(new Experiments(Catalog.all()), db, runs, tracing, streams);
  }

  @Bean
  RunComparison raceLabComparison(RunRepository runs) {
    return new RunComparison(runs);
  }

  @Bean
  RunEventStream raceLabEventStream(RunStreams streams, RunRepository runs) {
    return new RunEventStream(streams, runs);
  }
}
