package io.zeroshift.payment.infrastructure;

import io.github.resilience4j.circuitbreaker.*;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.payment.application.*;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Inbox;
import io.zeroshift.platform.Outbox;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
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

  /** Timeout, retry schedule and breaker use: three attempts backing off from 200 ms by default. */
  @Bean
  GatewayPolicy gatewayPolicy(
      CircuitBreaker gatewayBreaker,
      MeterRegistry meters,
      KafkaListenerEndpointRegistry consumers) {
    return new GatewayPolicy(gatewayBreaker, meters, consumers);
  }

  /**
   * Operator levers go to WireMock's admin API directly, never through the chaos proxy that charges
   * may be sent through, so making the network slow does not slow the levers.
   */
  @Bean
  GatewaySimulator gatewaySimulator(
      URI gatewayUri, @Value("${payment.gateway-admin-url:}") String adminUrl) {
    return new GatewaySimulator(adminUrl.isBlank() ? gatewayUri : URI.create(adminUrl));
  }

  @Bean
  GatewayCalls gatewayCalls(DSLContext db, TransactionTemplate transactions) {
    return new GatewayCalls(db, transactions);
  }

  @Bean
  PaymentGateway paymentGateway(
      URI gatewayUri,
      @Value("${payment.gateway-timeout:1500ms}") Duration connectTimeout,
      GatewayPolicy policy,
      CircuitBreaker gatewayBreaker,
      GatewayCalls calls,
      MeterRegistry meters) {
    return new HttpPaymentGateway(
        gatewayUri, connectTimeout, policy, gatewayBreaker, calls, meters);
  }

  @Bean
  Payments payments(DSLContext db) {
    return new PostgresPayments(db);
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
