package io.zeroshift.payment.infrastructure;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.payment.application.PaymentGateway;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import tools.jackson.databind.json.JsonMapper;

/**
 * The gateway over HTTP, guarded in two layers. Each attempt has a hard timeout; failed attempts
 * are retried on a schedule (Retry); every attempt is counted by a circuit breaker which, once half
 * of the recent calls failed, refuses calls outright for a while so a struggling gateway gets room
 * to recover. A declined card is an answer, not a failure: never retried or counted. Timeout,
 * schedule and breaker come from {@link GatewayPolicy}, which the resilience lab changes live.
 */
public final class HttpPaymentGateway implements PaymentGateway {
  private final HttpClient http;
  private final URI baseUri;
  private final GatewayPolicy policy;
  private final CircuitBreaker breaker;
  private final GatewayCalls calls;
  private final MeterRegistry meters;
  private final JsonMapper json = JsonMapper.builder().build();

  public HttpPaymentGateway(
      URI baseUri,
      Duration connectTimeout,
      GatewayPolicy policy,
      CircuitBreaker breaker,
      GatewayCalls calls,
      MeterRegistry meters) {
    this.baseUri = baseUri;
    this.policy = policy;
    this.breaker = breaker;
    this.calls = calls;
    this.meters = meters;
    http = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
  }

  @Override
  public Result charge(UUID orderId, String idempotencyKey, BigDecimal amount, String currency) {
    Supplier<Result> call = () -> attempt(orderId, idempotencyKey, amount, currency);
    if (policy.breakerEnabled()) call = CircuitBreaker.decorateSupplier(breaker, call);
    try {
      var result = Retry.decorateSupplier(policy.retry(), call).get();
      charge(result instanceof Charged ? "charged" : "declined");
      return result;
    } catch (CallNotPermittedException open) {
      charge("rejected_by_breaker");
      record(orderId, "REJECTED_BY_BREAKER", 0, "circuit open: not called");
      throw new Unavailable("Circuit breaker OPEN: gateway not called");
    } catch (Unavailable e) {
      charge("failed");
      throw e;
    }
  }

  private Result attempt(UUID orderId, String idempotencyKey, BigDecimal amount, String currency) {
    var timeout = policy.timeout();
    long started = System.nanoTime();
    try {
      var body =
          json.writeValueAsString(
              Map.of("orderId", orderId, "amount", amount, "currency", currency));
      var response =
          http.send(
              HttpRequest.newBuilder(baseUri.resolve("/charges"))
                  .timeout(timeout)
                  .header("Content-Type", "application/json")
                  // A retried charge must not charge twice: the gateway dedupes on this key.
                  .header("Idempotency-Key", idempotencyKey)
                  .POST(HttpRequest.BodyPublishers.ofString(body))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      long millis = elapsed(started);
      int status = response.statusCode();
      if (status / 100 == 2) {
        var reference = json.readTree(response.body()).path("reference").asString("unknown");
        record(orderId, "CHARGED", millis, reference);
        return new Charged(reference);
      }
      if (status == 402) {
        var reason = json.readTree(response.body()).path("error").asString("card declined");
        record(orderId, "DECLINED", millis, reason);
        return new Declined(reason);
      }
      record(orderId, "HTTP_" + status, millis, "gateway error");
      throw new Unavailable("Gateway answered HTTP " + status);
    } catch (HttpTimeoutException e) {
      record(
          orderId, "TIMEOUT", elapsed(started), "no answer within " + timeout.toMillis() + " ms");
      throw new Unavailable("Gateway timed out after " + timeout.toMillis() + " ms");
    } catch (IOException e) {
      record(orderId, "CONNECTION_FAILED", elapsed(started), e.toString());
      throw new Unavailable("Gateway unreachable: " + e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new Unavailable("Interrupted");
    }
  }

  /** One attempt that reached (or tried to reach) the gateway, logged and counted. */
  private void record(UUID orderId, String outcome, long millis, String detail) {
    meters.counter("zeroshift.gateway.attempts", "outcome", outcome).increment();
    calls.record(orderId, outcome, millis, state(), detail);
  }

  /** One charge as the handler sees it, after all its attempts. */
  private void charge(String result) {
    meters.counter("zeroshift.gateway.charges", "result", result).increment();
  }

  private String state() {
    return policy.breakerEnabled() ? breaker.getState().name() : "DISABLED";
  }

  private static long elapsed(long started) {
    return (System.nanoTime() - started) / 1_000_000;
  }
}
