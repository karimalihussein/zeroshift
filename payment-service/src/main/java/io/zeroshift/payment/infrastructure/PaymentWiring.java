package io.zeroshift.payment.infrastructure;

import io.github.resilience4j.circuitbreaker.*;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.github.resilience4j.micrometer.tagged.TaggedRetryMetrics;
import io.github.resilience4j.retry.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.payment.application.*;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Inbox;
import io.zeroshift.platform.Outbox;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
@Import(GatewayControl.class)
public class PaymentWiring {
  @Bean
  URI gatewayUri(@Value("${payment.gateway-url}") String url) {
    return URI.create(url);
  }

  @Bean
  CircuitBreaker gatewayBreaker(MeterRegistry meters) {
    var registry =
        CircuitBreakerRegistry.of(
            CircuitBreakerConfig.custom()
                .slidingWindowSize(10)
                .minimumNumberOfCalls(4)
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(15))
                .permittedNumberOfCallsInHalfOpenState(2)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordExceptions(PaymentGateway.Unavailable.class)
                .build());
    TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(meters);
    return registry.circuitBreaker("payment-gateway");
  }

  @Bean
  Retry gatewayRetry(MeterRegistry meters) {
    var registry =
        RetryRegistry.of(
            RetryConfig.custom()
                .maxAttempts(3)
                .intervalFunction(IntervalFunction.ofExponentialBackoff(200, 2))
                .retryExceptions(PaymentGateway.Unavailable.class)
                // An open breaker is a decision, not a glitch: do not hammer it again.
                .ignoreExceptions(CallNotPermittedException.class)
                .build());
    TaggedRetryMetrics.ofRetryRegistry(registry).bindTo(meters);
    return registry.retry("payment-gateway");
  }

  @Bean
  GatewayCalls gatewayCalls(JdbcTemplate jdbc, TransactionTemplate transactions) {
    return new GatewayCalls(jdbc, transactions);
  }

  @Bean
  PaymentGateway paymentGateway(
      URI gatewayUri,
      @Value("${payment.gateway-timeout:1500ms}") Duration timeout,
      CircuitBreaker gatewayBreaker,
      Retry gatewayRetry,
      GatewayCalls calls) {
    return new HttpPaymentGateway(gatewayUri, timeout, gatewayBreaker, gatewayRetry, calls);
  }

  @Bean
  Payments payments(JdbcTemplate jdbc) {
    return new JdbcPayments(jdbc);
  }

  @Bean
  PaymentHandler paymentHandler(
      Payments payments,
      PaymentGateway gateway,
      Outbox outbox,
      Faults faults,
      @Value("${payment.limit:1000.00}") BigDecimal limit) {
    return new PaymentHandler(payments, gateway, outbox, faults, limit);
  }

  @Bean
  PaymentCommands paymentCommands(Inbox inbox, PaymentHandler handler) {
    return new PaymentCommands(inbox, handler);
  }
}
