package io.zeroshift.payment.infrastructure;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.zeroshift.platform.web.ApiResponse;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.List;
import java.util.UUID;
import org.springframework.web.bind.annotation.*;

/** Operator levers on the simulated gateway, and the breaker's live state. */
@RestController
@RequestMapping("/lab/gateway")
public class GatewayControl {
  public record State(
      String mode,
      String breakerState,
      float failureRate,
      int bufferedCalls,
      int failedCalls,
      long notPermittedCalls,
      List<GatewayCalls.Call> recentCalls) {}

  private final GatewaySimulator gateway;
  private final CircuitBreaker breaker;
  private final GatewayCalls calls;

  public GatewayControl(GatewaySimulator gateway, CircuitBreaker breaker, GatewayCalls calls) {
    this.gateway = gateway;
    this.breaker = breaker;
    this.calls = calls;
  }

  @GetMapping
  public State state() {
    var m = breaker.getMetrics();
    return new State(
        gateway.mode().label(),
        breaker.getState().name(),
        m.getFailureRate(),
        m.getNumberOfBufferedCalls(),
        m.getNumberOfFailedCalls(),
        m.getNumberOfNotPermittedCalls(),
        calls.recent(null, 30));
  }

  @GetMapping("/calls")
  public ApiResponse<List<GatewayCalls.Call>> calls(
      @RequestParam(required = false) UUID orderId,
      @RequestParam(defaultValue = "100") @Min(1) @Max(500) int limit) {
    return ApiResponse.list(calls.recent(orderId, limit));
  }

  @PostMapping("/{mode}")
  public State set(@PathVariable String mode) {
    gateway.set(GatewaySimulator.Mode.parse(mode));
    return state();
  }

  @PostMapping("/breaker/reset")
  public State resetBreaker() {
    breaker.reset();
    return state();
  }
}
