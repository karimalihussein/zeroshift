package io.zeroshift.resilience;

import io.zeroshift.resilience.Experiment.Metric;

/** The quantities experiments reason about, each read from one {@link Sample}. */
final class Metrics {
  static final Metric OFFERED = s -> (double) s.traffic().offered();
  static final Metric ATTEMPTS = s -> (double) s.traffic().attempts();
  static final Metric SUCCEEDED = s -> (double) s.traffic().succeeded();
  static final Metric FAILED = s -> (double) s.traffic().failed();
  static final Metric THROTTLED = s -> (double) s.traffic().throttled();
  static final Metric CLIENT_ERRORS = s -> (double) s.traffic().errors();
  static final Metric WRITE_P99 = s -> number(s.latency().writeP99());
  static final Metric READ_P99 = s -> number(s.latency().readP99());
  static final Metric EDGE_IN_FLIGHT = s -> number(s.edge().inFlight());
  static final Metric SHED = s -> s.edge().shed();
  static final Metric RATE_LIMITED = s -> s.edge().rateLimited();
  static final Metric BULKHEAD_FULL = s -> s.edge().bulkheadFull();
  static final Metric ACTIVE_SAGAS =
      s -> s.sagas().active() == null ? null : s.sagas().active().doubleValue();
  static final Metric COMPLETED = s -> s.sagas().completed();
  static final Metric CANCELLED = s -> s.sagas().cancelled();
  static final Metric E2E_P99 = s -> s.sagas().p99();
  static final Metric RELAY_P95 =
      s -> s.relay().p95() == null ? null : s.relay().p95().doubleValue();
  static final Metric REBALANCES = s -> s.consumers().rebalances();
  static final Metric DEAD_LETTERED = s -> s.consumers().deadLettered();
  static final Metric DUPLICATES = s -> s.consumers().duplicates();
  static final Metric BREAKER_OPEN =
      s -> s.gateway().breaker() == null ? null : "OPEN".equals(s.gateway().breaker()) ? 1.0 : 0.0;
  static final Metric BREAKER_CLOSED =
      s ->
          s.gateway().breaker() == null ? null : "CLOSED".equals(s.gateway().breaker()) ? 1.0 : 0.0;
  static final Metric PAYMENT_PAUSED =
      s -> s.gateway().consumerPaused() == null ? null : s.gateway().consumerPaused() ? 1.0 : 0.0;

  /** Attempts sent per client that arrived: 1.0 means no retries, 5.0 means five tries each. */
  static final Metric AMPLIFICATION =
      s ->
          s.traffic().offered() == 0
              ? null
              : (double) s.traffic().attempts() / s.traffic().offered();

  /** Gateway attempts per second that actually reached (or tried to reach) the gateway. */
  static final Metric GATEWAY_CALLS =
      s ->
          s.gateway().attempts().entrySet().stream()
              .filter(e -> !e.getKey().equals("REJECTED_BY_BREAKER"))
              .mapToDouble(java.util.Map.Entry::getValue)
              .sum();

  static Metric lag(String group) {
    return s ->
        s.lag() == null || !s.lag().containsKey(group) ? null : s.lag().get(group).doubleValue();
  }

  static Metric poolPending(String service) {
    return s -> {
      var r = s.resources().get(service);
      return r == null ? null : (double) r.poolPending();
    };
  }

  private static Double number(Integer value) {
    return value == null ? null : value.doubleValue();
  }

  private Metrics() {}
}
