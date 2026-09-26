package io.zeroshift.payment.infrastructure;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.micrometer.tagged.TaggedRetryMetrics;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.payment.application.PaymentGateway;
import io.zeroshift.platform.web.ApiException;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

/**
 * How the payment service calls the gateway, changeable while it runs: the per-attempt timeout, the
 * retry schedule, whether the circuit breaker guards the calls, and whether an open breaker pauses
 * the payment consumer (backpressure into Kafka) instead of failing every command.
 */
public final class GatewayPolicy {
  /** How retries are spaced. {@code IMMEDIATE} retries at once: the recipe for a retry storm. */
  public enum RetryMode {
    NONE,
    IMMEDIATE,
    EXPONENTIAL,
    JITTER;

    static RetryMode parse(String value) {
      try {
        return valueOf(value.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException | NullPointerException e) {
        throw new ApiException(
            HttpStatus.BAD_REQUEST,
            "UNKNOWN_RETRY_MODE",
            "Retry mode is none, immediate, exponential or jitter, not " + value);
      }
    }
  }

  public record Settings(
      int timeoutMs, String retry, int maxAttempts, boolean breaker, boolean pauseOnOpen) {
    public static final Settings DEFAULT = new Settings(1500, "exponential", 3, true, false);

    public Settings {
      RetryMode.parse(retry);
      if (timeoutMs < 50 || timeoutMs > 60_000 || maxAttempts < 1 || maxAttempts > 10)
        throw new ApiException(
            HttpStatus.BAD_REQUEST,
            "INVALID_GATEWAY_POLICY",
            "Timeout is 50–60000 ms and attempts 1–10");
    }
  }

  private final CircuitBreaker breaker;
  private final MeterRegistry meters;
  private final KafkaListenerEndpointRegistry consumers;
  private static final String NAME = "payment-gateway";
  private final RetryRegistry retries = RetryRegistry.ofDefaults();
  private final AtomicLong pauses = new AtomicLong();
  private volatile Settings settings;
  private volatile Retry retry;

  public GatewayPolicy(
      CircuitBreaker breaker, MeterRegistry meters, KafkaListenerEndpointRegistry consumers) {
    this.breaker = breaker;
    this.meters = meters;
    this.consumers = consumers;
    TaggedRetryMetrics.ofRetryRegistry(retries).bindTo(meters);
    apply(Settings.DEFAULT);
    breaker
        .getEventPublisher()
        .onStateTransition(e -> onBreaker(e.getStateTransition().getToState()));
  }

  public Settings settings() {
    return settings;
  }

  public Duration timeout() {
    return Duration.ofMillis(settings.timeoutMs());
  }

  public Retry retry() {
    return retry;
  }

  public boolean breakerEnabled() {
    return settings.breaker();
  }

  /** How many times an open breaker has paused the consumer. */
  public long pauses() {
    return pauses.get();
  }

  public synchronized void apply(Settings next) {
    // Replaced, not reconfigured: a Retry's schedule is fixed. The registry re-tags its metrics.
    retries.remove(NAME);
    retry = retries.retry(NAME, retryConfig(next));
    var wasPausing = settings != null && settings.pauseOnOpen();
    settings = next;
    if (wasPausing && !next.pauseOnOpen()) resumeConsumer();
    if (next.pauseOnOpen() && breaker.getState() == CircuitBreaker.State.OPEN) pauseConsumer();
  }

  static RetryConfig retryConfig(Settings s) {
    var mode = RetryMode.parse(s.retry());
    var builder =
        RetryConfig.custom()
            .maxAttempts(mode == RetryMode.NONE ? 1 : s.maxAttempts())
            .retryExceptions(PaymentGateway.Unavailable.class)
            // An open breaker is a decision, not a glitch: do not hammer it again.
            .ignoreExceptions(CallNotPermittedException.class);
    return builder
        .intervalFunction(
            switch (mode) {
              case NONE, IMMEDIATE -> IntervalFunction.of(Duration.ofMillis(1));
              case EXPONENTIAL -> IntervalFunction.ofExponentialBackoff(200, 2);
              // Each wait is drawn from ±50 % around the exponential value, so clients that
              // failed together do not all come back together.
              case JITTER -> IntervalFunction.ofExponentialRandomBackoff(200, 2, 0.5);
            })
        .build();
  }

  private void onBreaker(CircuitBreaker.State to) {
    if (!settings.pauseOnOpen()) return;
    if (to == CircuitBreaker.State.OPEN) pauseConsumer();
    // Half-open lets a few real commands through as probes; closed means the gateway is back.
    else if (to == CircuitBreaker.State.HALF_OPEN || to == CircuitBreaker.State.CLOSED)
      resumeConsumer();
  }

  private void pauseConsumer() {
    var container = consumers.getListenerContainer(PaymentCommands.CONSUMER);
    if (container != null && !container.isPauseRequested()) {
      container.pause();
      pauses.incrementAndGet();
      meters.counter("zeroshift.gateway.consumer_pauses").increment();
    }
  }

  private void resumeConsumer() {
    var container = consumers.getListenerContainer(PaymentCommands.CONSUMER);
    if (container != null && container.isPauseRequested()) container.resume();
  }
}
