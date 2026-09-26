package io.zeroshift.platform;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Statistic;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * One service's resource pressure and counters, read from its own Micrometer registry: the same
 * meters Prometheus scrapes, without waiting for a scrape. Counters are running totals; the control
 * plane turns consecutive readings into rates.
 */
public final class LabPressure {
  /**
   * @param rebalances this JVM's consumer rebalances since start, across all its consumers
   * @param recordsConsumed records this JVM's consumers have fetched since start
   * @param maxRecordsLag the largest per-partition lag any of this JVM's consumers reports
   * @param decisions consumer decisions since start, by decision
   * @param counters every {@code zeroshift.*} meter, keyed {@code name{tag=value,...}}
   */
  public record Pressure(
      String service,
      String instance,
      double processCpu,
      double systemCpu,
      long heapUsedBytes,
      long heapMaxBytes,
      int liveThreads,
      Pool pool,
      double rebalances,
      double recordsConsumed,
      double maxRecordsLag,
      Map<String, Double> decisions,
      Map<String, Double> counters) {}

  /** The Hikari connection pool: busy, idle, threads waiting for a connection, and the size. */
  public record Pool(int active, int idle, int pending, int max, double timeouts) {}

  private final MeterRegistry meters;
  private final String service;
  private final String instance;

  public LabPressure(MeterRegistry meters, String service, String instance) {
    this.meters = meters;
    this.service = service;
    this.instance = instance;
  }

  public Pressure read() {
    return new Pressure(
        service,
        instance,
        sum("process.cpu.usage", m -> true),
        sum("system.cpu.usage", m -> true),
        (long) sum("jvm.memory.used", heap()),
        (long) sum("jvm.memory.max", heap()),
        (int) sum("jvm.threads.live", m -> true),
        new Pool(
            (int) sum("hikaricp.connections.active", m -> true),
            (int) sum("hikaricp.connections.idle", m -> true),
            (int) sum("hikaricp.connections.pending", m -> true),
            (int) sum("hikaricp.connections.max", m -> true),
            sum("hikaricp.connections.timeout", m -> true)),
        sum("kafka.consumer.coordinator.rebalance.total", m -> true),
        sum("kafka.consumer.fetch.manager.records.consumed.total", noTopicTag()),
        max("kafka.consumer.fetch.manager.records.lag.max"),
        meters.find("zeroshift.consumer.decisions").meters().stream()
            .collect(
                Collectors.groupingBy(
                    m -> m.getId().getTag("decision"),
                    TreeMap::new,
                    Collectors.summingDouble(LabPressure::value))),
        meters.getMeters().stream()
            .filter(m -> m.getId().getName().startsWith("zeroshift."))
            .collect(
                Collectors.toMap(LabPressure::key, LabPressure::value, Double::sum, TreeMap::new)));
  }

  private static Predicate<Meter> heap() {
    return m -> "heap".equals(m.getId().getTag("area"));
  }

  /** Kafka reports consumed records per topic and in total; only the totals, not both. */
  private static Predicate<Meter> noTopicTag() {
    return m -> m.getId().getTag("topic") == null;
  }

  private double sum(String name, Predicate<Meter> filter) {
    return meters.find(name).meters().stream()
        .filter(filter)
        .mapToDouble(LabPressure::value)
        .filter(Double::isFinite)
        .sum();
  }

  private double max(String name) {
    return meters.find(name).meters().stream()
        .mapToDouble(LabPressure::value)
        .filter(Double::isFinite)
        .max()
        .orElse(0);
  }

  /** A counter's count, a gauge's value, a timer's count: the meter's first measurement. */
  static double value(Meter meter) {
    for (var m : meter.measure())
      if (m.getStatistic() == Statistic.COUNT
          || m.getStatistic() == Statistic.TOTAL
          || m.getStatistic() == Statistic.VALUE
          || m.getStatistic() == Statistic.ACTIVE_TASKS) return m.getValue();
    return Double.NaN;
  }

  static String key(Meter meter) {
    var id = meter.getId();
    if (id.getTags().isEmpty()) return id.getName();
    return id.getName()
        + id.getTags().stream()
            .map(t -> t.getKey() + "=" + t.getValue())
            .sorted()
            .collect(Collectors.joining(",", "{", "}"));
  }
}
