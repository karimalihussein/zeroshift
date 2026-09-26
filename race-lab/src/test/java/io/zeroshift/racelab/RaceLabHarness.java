package io.zeroshift.racelab;

import com.zaxxer.hikari.HikariDataSource;
import io.zeroshift.racelab.application.ExperimentEngine;
import io.zeroshift.racelab.application.Experiments;
import io.zeroshift.racelab.application.RunStreams;
import io.zeroshift.racelab.application.port.Tracing;
import io.zeroshift.racelab.domain.EventType;
import io.zeroshift.racelab.domain.Interleaving;
import io.zeroshift.racelab.domain.Isolation;
import io.zeroshift.racelab.domain.Mode;
import io.zeroshift.racelab.domain.RaceEvent;
import io.zeroshift.racelab.domain.Run;
import io.zeroshift.racelab.domain.RunConfig;
import io.zeroshift.racelab.experiments.Catalog;
import io.zeroshift.racelab.infrastructure.postgres.PostgresLabDatabase;
import io.zeroshift.racelab.infrastructure.postgres.PostgresRunRepository;
import io.zeroshift.racelab.infrastructure.postgres.RaceLabSchema;
import java.time.Duration;
import java.util.List;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * The lab wired as the application wires it, on a real PostgreSQL 17 container: real connections,
 * real locks, real aborts. Nothing about concurrency is mocked.
 */
final class RaceLabHarness implements AutoCloseable {
  final PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:17-alpine")
          .withCommand("postgres", "-c", "max_connections=200");
  final HikariDataSource pool;
  final PostgresLabDatabase db;
  final PostgresRunRepository runs;
  final ExperimentEngine engine;
  final RunStreams streams = new RunStreams();

  RaceLabHarness() {
    postgres.start();
    pool =
        RaceLabSchema.pool(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    RaceLabSchema.migrate(pool);
    db = new PostgresLabDatabase(pool);
    runs = new PostgresRunRepository(pool);
    engine = new ExperimentEngine(new Experiments(Catalog.all()), db, runs, Tracing.NONE, streams);
  }

  /** Creates, starts and waits for a run. */
  Run run(
      String experiment,
      Mode mode,
      Isolation isolation,
      int requests,
      int value,
      int delayMs,
      int retries) {
    return run(experiment, mode, isolation, requests, value, delayMs, retries, null);
  }

  /** As above, with {@code listener} subscribed to the run's live events before it starts. */
  Run run(
      String experiment,
      Mode mode,
      Isolation isolation,
      int requests,
      int value,
      int delayMs,
      int retries,
      RunStreams.Listener listener) {
    var created =
        engine.create(
            new RunConfig(
                experiment,
                mode,
                isolation,
                requests,
                value,
                delayMs,
                Interleaving.CONTROLLED,
                retries));
    if (listener != null) streams.subscribe(created.id(), listener);
    engine.start(created.id());
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (System.nanoTime() < deadline) {
      var run = runs.find(created.id()).orElseThrow();
      if (run.status() == Run.Status.COMPLETED || run.status() == Run.Status.FAILED) {
        if (run.status() == Run.Status.FAILED)
          throw new AssertionError("run failed: " + run.error());
        return run;
      }
      sleep(50);
    }
    throw new AssertionError("run " + created.id() + " did not finish");
  }

  Run run(String experiment, Mode mode) {
    return run(experiment, mode, null, 0, Integer.MIN_VALUE, -1, -1);
  }

  List<RaceEvent> events(Run run) {
    return runs.events(run.id(), 0, 10_000);
  }

  List<RaceEvent> events(Run run, EventType type) {
    return events(run).stream().filter(e -> e.type() == type).toList();
  }

  static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    engine.close();
    pool.close();
    postgres.stop();
  }
}
