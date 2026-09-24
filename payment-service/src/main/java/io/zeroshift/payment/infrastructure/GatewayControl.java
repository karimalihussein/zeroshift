package io.zeroshift.payment.infrastructure;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.net.URI;
import java.net.http.*;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/**
 * Operator levers on the gateway itself (WireMock's admin API) and the breaker's live state. The
 * gateway really becomes slow or down; the payment service only notices through its calls.
 */
@RestController
@RequestMapping("/lab/gateway")
public class GatewayControl {
  private final HttpClient http = HttpClient.newHttpClient();
  private final URI gateway;
  private final CircuitBreaker breaker;
  private final GatewayCalls calls;
  private volatile String mode = "healthy";

  public GatewayControl(URI gateway, CircuitBreaker breaker, GatewayCalls calls) {
    this.gateway = gateway;
    this.breaker = breaker;
    this.calls = calls;
  }

  @GetMapping
  public Map<String, Object> state() {
    var m = breaker.getMetrics();
    return Map.of(
        "mode", mode,
        "breakerState", breaker.getState().name(),
        "failureRate", m.getFailureRate(),
        "bufferedCalls", m.getNumberOfBufferedCalls(),
        "failedCalls", m.getNumberOfFailedCalls(),
        "notPermittedCalls", m.getNumberOfNotPermittedCalls(),
        "recentCalls", calls.recent(null, 30));
  }

  @GetMapping("/calls")
  public java.util.List<Map<String, Object>> calls(
      @RequestParam(required = false) java.util.UUID orderId) {
    return calls.recent(orderId, 100);
  }

  @PostMapping("/{mode}")
  public Map<String, Object> set(@PathVariable String mode) {
    admin("POST", "/__admin/mappings/reset", "");
    switch (mode) {
      case "healthy" -> admin("POST", "/__admin/settings", "{\"fixedDelay\":0}");
      case "slow" -> admin("POST", "/__admin/settings", "{\"fixedDelay\":3000}");
      case "down" -> stub("{\"status\":503,\"body\":\"maintenance\"}");
      case "declining" ->
          stub(
              "{\"status\":402,\"headers\":{\"Content-Type\":\"application/json\"},"
                  + "\"jsonBody\":{\"error\":\"card declined by issuer\"}}");
      default ->
          throw new ResponseStatusException(
              HttpStatus.BAD_REQUEST, "healthy, slow, down or declining");
    }
    if (!mode.equals("slow")) admin("POST", "/__admin/settings", "{\"fixedDelay\":0}");
    this.mode = mode;
    return state();
  }

  @PostMapping("/breaker/reset")
  public Map<String, Object> resetBreaker() {
    breaker.reset();
    return state();
  }

  private void stub(String response) {
    admin(
        "POST",
        "/__admin/mappings",
        "{\"priority\":1,\"request\":{\"method\":\"POST\",\"urlPath\":\"/charges\"},\"response\":"
            + response
            + "}");
  }

  private void admin(String method, String path, String body) {
    try {
      var response =
          http.send(
              HttpRequest.newBuilder(gateway.resolve(path))
                  .header("Content-Type", "application/json")
                  .method(method, HttpRequest.BodyPublishers.ofString(body))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() / 100 != 2)
        throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "WireMock: " + response.body());
    } catch (java.io.IOException e) {
      throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Gateway admin unreachable: " + e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
