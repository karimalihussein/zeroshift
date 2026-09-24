package io.zeroshift.infrastructure;

import io.micrometer.core.instrument.*;
import io.zeroshift.application.ChangeCatchUp;
import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

/**
 * Publishes the lab to Micrometer. Counts, progress and routing are read from the durable store on
 * each scrape, so meters agree with the dashboard and survive restarts; only timings are
 * in-process.
 */
public final class MicrometerMigrationMetrics implements MigrationMetrics {
  static final String BATCH_DURATION = "zeroshift.migration.batch.duration";

  private final MeterRegistry registry;
  private final Timer cutover;

  public MicrometerMigrationMetrics(
      MeterRegistry registry, MigrationStore store, ChangeCatchUp catchUp) {
    this.registry = registry;
    cutover =
        Timer.builder("zeroshift.cutover.duration")
            .description("Final fence, catch-up, validation and sequence sync")
            .register(registry);

    counter(
        registry,
        "zeroshift.migration.rows.copied",
        "Snapshot rows committed",
        store,
        s -> s.state().copied());
    gauge(
        registry,
        "zeroshift.migration.rows.per.second",
        "Rate of the last snapshot batch",
        store,
        s -> s.state().rowsPerSecond());
    counter(
        registry,
        "zeroshift.cdc.changes.applied",
        "Net changes replayed to PostgreSQL",
        store,
        s -> s.state().applied());
    // Opens a Change Tracking capture per scrape: the same real query the dashboard runs.
    gauge(
        registry,
        "zeroshift.cdc.changes.pending",
        "Captured changes not yet replayed",
        store,
        catchUp::pending);
    gauge(
        registry,
        "zeroshift.reverse.changes.pending",
        "PostgreSQL keys captured after cutover and not yet replayed into SQL Server",
        store,
        MigrationStore::reversePending);
    for (var stage : Stage.values())
      Gauge.builder("zeroshift.migration.stage", store, s -> s.state().stage() == stage ? 1 : 0)
          .tag("stage", tag(stage))
          .register(registry);
    for (var status : RunStatus.values())
      Gauge.builder("zeroshift.migration.status", store, s -> s.state().status() == status ? 1 : 0)
          .tag("status", tag(status))
          .register(registry);
    for (var primary : Primary.values())
      Gauge.builder("zeroshift.primary", store, s -> s.state().primary() == primary ? 1 : 0)
          .description("1 for the database that receives application writes")
          .tag("database", tag(primary))
          .register(registry);

    for (var operation : TrafficOperation.values())
      FunctionCounter.builder(
              "zeroshift.traffic.operations", store, s -> count(s.trafficMetrics(), operation))
          .description("Committed simulated operations")
          .tag("operation", tag(operation))
          .register(registry);
    counter(
        registry,
        "zeroshift.traffic.errors",
        "Failed simulated operations",
        store,
        s -> s.trafficMetrics().errors());
    gauge(
        registry,
        "zeroshift.traffic.operations.per.second",
        "Committed operations in the last ~1s",
        store,
        s -> s.trafficMetrics().operationsPerSecond());
    gauge(
        registry,
        "zeroshift.traffic.running",
        "1 while live traffic is enabled",
        store,
        s -> s.trafficMetrics().running() ? 1 : 0);
  }

  @Override
  public void batchCopied(Table table, Duration elapsed) {
    Timer.builder(BATCH_DURATION)
        .description("Read, COPY and checkpoint of one snapshot batch")
        .tag("table", tag(table))
        .register(registry)
        .record(elapsed);
  }

  @Override
  public void cutoverFinished(Duration elapsed) {
    cutover.record(elapsed);
  }

  @Override
  public BatchTiming batchTiming() {
    var timers = registry.find(BATCH_DURATION).timers();
    long count = timers.stream().mapToLong(Timer::count).sum();
    double total = timers.stream().mapToDouble(t -> t.totalTime(TimeUnit.MILLISECONDS)).sum();
    double max = timers.stream().mapToDouble(t -> t.max(TimeUnit.MILLISECONDS)).max().orElse(0);
    return new BatchTiming(count, count == 0 ? 0 : total / count, max);
  }

  private static long count(TrafficMetrics metrics, TrafficOperation operation) {
    return switch (operation) {
      case INSERT -> metrics.inserts();
      case UPDATE -> metrics.updates();
      case DELETE -> metrics.deletes();
      case READ -> metrics.reads();
    };
  }

  private static String tag(Enum<?> value) {
    return value.name().toLowerCase(Locale.ROOT);
  }

  private static <T> void counter(
      MeterRegistry registry,
      String name,
      String description,
      T source,
      ToDoubleFunction<T> value) {
    FunctionCounter.builder(name, source, value).description(description).register(registry);
  }

  private static <T> void gauge(
      MeterRegistry registry,
      String name,
      String description,
      T source,
      ToDoubleFunction<T> value) {
    Gauge.builder(name, source, value).description(description).register(registry);
  }
}
