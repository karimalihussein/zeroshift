package io.zeroshift.order.web;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.order.infrastructure.SagaPressure;
import io.zeroshift.platform.web.ApiErrors;
import io.zeroshift.platform.web.ApiException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Admission control for POST /orders, three independent guards an operator switches on and sizes at
 * runtime. Each refusal is immediate and says why, so a client can back off instead of waiting:
 *
 * <ul>
 *   <li><b>Rate limiter</b>: at most N admissions per second on this replica; the rest get 429.
 *   <li><b>Load shedder</b>: while more than N sagas are unfinished (work already inside the
 *       system, waiting on Kafka consumers downstream), new orders get 503. It reacts to what the
 *       system is carrying, not to how fast requests arrive.
 *   <li><b>Bulkhead</b>: at most N placements run at once, so a slow database cannot let order
 *       placement take every connection and thread; reads keep the rest. Excess gets 503.
 * </ul>
 *
 * Settings are per replica and live in memory: two replicas each admitting 10/s admit 20/s.
 */
@Component
public class EdgeGuards {
  public static final String RATE_LIMITED = "RATE_LIMITED";
  public static final String LOAD_SHED = "LOAD_SHED";
  public static final String BULKHEAD_FULL = "BULKHEAD_FULL";

  public record Settings(
      boolean rateLimit,
      int ratePerSecond,
      boolean shedding,
      int maxActiveSagas,
      boolean bulkhead,
      int maxConcurrent) {
    public static final Settings OFF = new Settings(false, 20, false, 200, false, 6);

    public Settings {
      if (ratePerSecond < 1 || maxActiveSagas < 1 || maxConcurrent < 1)
        throw new ApiException(
            HttpStatus.BAD_REQUEST, "INVALID_GUARD_SETTINGS", "Limits must be at least 1");
    }
  }

  public record Counters(long admitted, long rateLimited, long shed, long bulkheadFull) {}

  public record State(
      Settings settings, Counters counters, int inFlight, SagaPressure.Active active) {}

  private final SagaPressure sagas;
  private volatile RateLimiter limiter;
  private final Bulkhead bulkhead;
  private final AtomicInteger inFlight = new AtomicInteger();
  private final Counter admitted;
  private final Counter rateLimited;
  private final Counter shed;
  private final Counter bulkheadFull;
  private volatile Settings settings = Settings.OFF;

  public EdgeGuards(SagaPressure sagas, MeterRegistry meters) {
    this.sagas = sagas;
    limiter = limiter(Settings.OFF.ratePerSecond());
    bulkhead = Bulkhead.of("orders-edge", bulkheadConfig(Settings.OFF.maxConcurrent()));
    admitted = outcome(meters, "admitted");
    rateLimited = outcome(meters, "rate_limited");
    shed = outcome(meters, "shed");
    bulkheadFull = outcome(meters, "bulkhead_full");
    Gauge.builder("zeroshift.edge.in_flight", inFlight, AtomicInteger::get)
        .description("POST /orders requests being handled now")
        .register(meters);
  }

  public State state() {
    return new State(
        settings,
        new Counters(
            (long) admitted.count(),
            (long) rateLimited.count(),
            (long) shed.count(),
            (long) bulkheadFull.count()),
        inFlight.get(),
        sagas.active());
  }

  public State configure(Settings next) {
    // A new limiter rather than changeLimitForPeriod, which only applies from the next period:
    // lowering the limit should bite at once.
    limiter = limiter(next.ratePerSecond());
    bulkhead.changeConfig(bulkheadConfig(next.maxConcurrent()));
    settings = next;
    return state();
  }

  /** Runs {@code placement} if every enabled guard admits it; otherwise refuses with the reason. */
  public <T> T admit(Callable<T> placement) throws Exception {
    var s = settings;
    if (s.rateLimit() && !limiter.acquirePermission()) {
      rateLimited.increment();
      throw refused(
          HttpStatus.TOO_MANY_REQUESTS,
          RATE_LIMITED,
          "Over " + s.ratePerSecond() + " orders/s on this replica",
          1);
    }
    if (s.shedding()) {
      long active = sagas.active().active();
      if (active > s.maxActiveSagas()) {
        shed.increment();
        throw refused(
            HttpStatus.SERVICE_UNAVAILABLE,
            LOAD_SHED,
            active + " orders still in progress (limit " + s.maxActiveSagas() + ")",
            2);
      }
    }
    boolean isolated = s.bulkhead();
    if (isolated && !bulkhead.tryAcquirePermission()) {
      bulkheadFull.increment();
      throw refused(
          HttpStatus.SERVICE_UNAVAILABLE,
          BULKHEAD_FULL,
          s.maxConcurrent() + " placements already running on this replica",
          1);
    }
    inFlight.incrementAndGet();
    try {
      var result = placement.call();
      admitted.increment();
      return result;
    } finally {
      inFlight.decrementAndGet();
      if (isolated) bulkhead.onComplete();
    }
  }

  private static ApiException refused(
      HttpStatus status, String code, String detail, int retryAfterSeconds) {
    return new ApiException(
        status, code, detail, Map.of(ApiErrors.RETRY_AFTER_SECONDS, retryAfterSeconds));
  }

  private static RateLimiter limiter(int perSecond) {
    return RateLimiter.of(
        "orders-edge",
        RateLimiterConfig.custom()
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .limitForPeriod(perSecond)
            .timeoutDuration(Duration.ZERO)
            .build());
  }

  private static BulkheadConfig bulkheadConfig(int maxConcurrent) {
    return BulkheadConfig.custom()
        .maxConcurrentCalls(maxConcurrent)
        .maxWaitDuration(Duration.ZERO)
        .build();
  }

  private static Counter outcome(MeterRegistry meters, String outcome) {
    return Counter.builder("zeroshift.edge.requests")
        .description("POST /orders by admission outcome")
        .tag("outcome", outcome)
        .register(meters);
  }
}
