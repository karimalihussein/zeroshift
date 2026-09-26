package io.zeroshift.resilience;

import java.util.List;
import java.util.Map;

/**
 * One second of the whole system, as measured: what clients sent and got, what the edge admitted,
 * how long orders took end to end, where Kafka lag built up, what consumers and the gateway did,
 * and how hard each JVM worked. Rates are per second over the interval since the previous sample. A
 * null means "not measured this second" (a service did not answer in time), never zero.
 */
public record Sample(
    long t,
    Traffic traffic,
    Latency latency,
    Edge edge,
    Sagas sagas,
    Map<String, Long> lag,
    Consumers consumers,
    Gateway gateway,
    Relay relay,
    Map<String, Resource> resources,
    List<String> unreachable) {

  /** The load generator's clients during this second. */
  public record Traffic(
      int offered,
      int dropped,
      int attempts,
      int retries,
      int succeeded,
      int failed,
      int throttled,
      Map<String, Integer> outcomes,
      int[] attemptsPerTenth,
      int inFlight) {
    public int errors() {
      int errors = 0;
      for (var e : outcomes.entrySet()) if (!e.getKey().equals("OK")) errors += e.getValue();
      return errors;
    }
  }

  /** Client-observed latency of successful requests over the last five seconds, in ms. */
  public record Latency(
      Integer writeP50,
      Integer writeP95,
      Integer writeP99,
      Integer readP50,
      Integer readP95,
      Integer readP99) {}

  /** POST /orders admissions on both order-service replicas, per second. */
  public record Edge(
      Double admitted, Double rateLimited, Double shed, Double bulkheadFull, Integer inFlight) {}

  /** Order sagas: finished per second, still open, and placement-to-end latency in ms. */
  public record Sagas(
      Double completed, Double cancelled, Long active, Double p50, Double p95, Double p99) {}

  /** Every service's consumers together, per second; rebalances are this second's new ones. */
  public record Consumers(
      Double processed,
      Double retries,
      Double deadLettered,
      Double duplicates,
      Double rebalances) {}

  /**
   * Payment → card gateway: attempts per second by outcome, the breaker, and whether the payment
   * consumer is paused (backpressure into Kafka).
   */
  public record Gateway(
      Map<String, Double> attempts,
      Double charges,
      String breaker,
      Boolean consumerPaused,
      Integer timeoutMs,
      String retry) {}

  /** Outbox → Debezium → Kafka delay of the records relayed this second, in ms. */
  public record Relay(int records, Long p95, Long max) {}

  /** One JVM. CPU is a percentage of one core; heap in MB; the Hikari pool in connections. */
  public record Resource(
      double cpu,
      long heapUsedMb,
      long heapMaxMb,
      int threads,
      int poolActive,
      int poolPending,
      int poolMax) {}
}
