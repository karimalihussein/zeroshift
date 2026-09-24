package io.zeroshift.payment.infrastructure;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.zeroshift.payment.application.PaymentGateway;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.UUID;
import tools.jackson.databind.json.JsonMapper;

/**
 * The gateway over HTTP, guarded in two layers. Each attempt has a hard timeout; up to three
 * attempts back off exponentially (Retry); every attempt is counted by a circuit breaker which,
 * once half of the recent calls failed, refuses calls outright for a while so a struggling gateway
 * gets room to recover. A declined card is an answer, not a failure: never retried or counted.
 */
public final class HttpPaymentGateway implements PaymentGateway {
  private final HttpClient http;
  private final URI baseUri;
  private final Duration timeout;
  private final CircuitBreaker breaker;
  private final Retry retry;
  private final GatewayCalls calls;
  private final JsonMapper json = JsonMapper.builder().build();

  public HttpPaymentGateway(
      URI baseUri, Duration timeout, CircuitBreaker breaker, Retry retry, GatewayCalls calls) {
    this.baseUri = baseUri;
    this.timeout = timeout;
    this.breaker = breaker;
    this.retry = retry;
    this.calls = calls;
    http = HttpClient.newBuilder().connectTimeout(timeout).build();
  }

  @Override
  public Result charge(UUID orderId, BigDecimal amount, String currency) {
    try {
      return Retry.decorateSupplier(
              retry,
              CircuitBreaker.decorateSupplier(breaker, () -> attempt(orderId, amount, currency)))
          .get();
    } catch (CallNotPermittedException open) {
      calls.record(
          orderId, "REJECTED_BY_BREAKER", 0, breaker.getState().name(), "circuit open: not called");
      throw new Unavailable("Circuit breaker OPEN: gateway not called");
    }
  }

  private Result attempt(UUID orderId, BigDecimal amount, String currency) {
    long started = System.nanoTime();
    try {
      var body =
          json.writeValueAsString(
              java.util.Map.of("orderId", orderId, "amount", amount, "currency", currency));
      var response =
          http.send(
              HttpRequest.newBuilder(baseUri.resolve("/charges"))
                  .timeout(timeout)
                  .header("Content-Type", "application/json")
                  // A retried charge must not charge twice: the gateway dedupes on this key.
                  .header("Idempotency-Key", orderId.toString())
                  .POST(HttpRequest.BodyPublishers.ofString(body))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      long millis = elapsed(started);
      int status = response.statusCode();
      if (status / 100 == 2) {
        var reference = json.readTree(response.body()).path("reference").asString("unknown");
        calls.record(orderId, "CHARGED", millis, breaker.getState().name(), reference);
        return new Charged(reference);
      }
      if (status == 402) {
        var reason = json.readTree(response.body()).path("error").asString("card declined");
        calls.record(orderId, "DECLINED", millis, breaker.getState().name(), reason);
        return new Declined(reason);
      }
      calls.record(orderId, "HTTP_" + status, millis, breaker.getState().name(), "gateway error");
      throw new Unavailable("Gateway answered HTTP " + status);
    } catch (HttpTimeoutException e) {
      calls.record(
          orderId,
          "TIMEOUT",
          elapsed(started),
          breaker.getState().name(),
          "no answer within " + timeout.toMillis() + " ms");
      throw new Unavailable("Gateway timed out after " + timeout.toMillis() + " ms");
    } catch (IOException e) {
      calls.record(
          orderId, "CONNECTION_FAILED", elapsed(started), breaker.getState().name(), e.toString());
      throw new Unavailable("Gateway unreachable: " + e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new Unavailable("Interrupted");
    }
  }

  private static long elapsed(long started) {
    return (System.nanoTime() - started) / 1_000_000;
  }
}
