package io.zeroshift.payment.infrastructure;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.zeroshift.platform.web.ApiException;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

class GatewayPolicyTest {
  private final CircuitBreaker breaker =
      CircuitBreaker.of("test", CircuitBreakerConfig.ofDefaults());
  private final KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
  private final MessageListenerContainer consumer = mock(MessageListenerContainer.class);
  private final GatewayPolicy policy;

  GatewayPolicyTest() {
    when(registry.getListenerContainer(PaymentCommands.CONSUMER)).thenReturn(consumer);
    policy = new GatewayPolicy(breaker, new SimpleMeterRegistry(), registry);
  }

  @Test
  void retrySchedules() {
    var none = GatewayPolicy.retryConfig(new GatewayPolicy.Settings(800, "none", 5, true, false));
    assertThat(none.getMaxAttempts()).isEqualTo(1);
    var exponential =
        GatewayPolicy.retryConfig(new GatewayPolicy.Settings(800, "exponential", 3, true, false));
    assertThat(exponential.getMaxAttempts()).isEqualTo(3);
    assertThat(exponential.getIntervalBiFunction().apply(1, null)).isEqualTo(200);
    assertThat(exponential.getIntervalBiFunction().apply(2, null)).isEqualTo(400);
    var jitter =
        GatewayPolicy.retryConfig(new GatewayPolicy.Settings(800, "jitter", 3, true, false));
    for (int i = 0; i < 100; i++)
      assertThat(jitter.getIntervalBiFunction().apply(2, null)).isBetween(200L, 600L);
  }

  @Test
  void appliesTimeoutAndRetryLive() {
    policy.apply(new GatewayPolicy.Settings(800, "immediate", 5, false, false));
    assertThat(policy.timeout().toMillis()).isEqualTo(800);
    assertThat(policy.retry().getRetryConfig().getMaxAttempts()).isEqualTo(5);
    assertThat(policy.breakerEnabled()).isFalse();
  }

  @Test
  void anOpenBreakerPausesTheConsumerOnlyWhenAskedTo() {
    breaker.transitionToOpenState();
    verify(consumer, never()).pause();

    breaker.transitionToClosedState();
    policy.apply(new GatewayPolicy.Settings(800, "jitter", 2, true, true));
    breaker.transitionToOpenState();
    verify(consumer).pause();
    assertThat(policy.pauses()).isEqualTo(1);

    when(consumer.isPauseRequested()).thenReturn(true);
    breaker.transitionToHalfOpenState();
    verify(consumer).resume();
  }

  @Test
  void turningPauseOffResumesAPausedConsumer() {
    policy.apply(new GatewayPolicy.Settings(800, "jitter", 2, true, true));
    breaker.transitionToOpenState();
    when(consumer.isPauseRequested()).thenReturn(true);
    policy.apply(GatewayPolicy.Settings.DEFAULT);
    verify(consumer).resume();
  }

  @Test
  void rejectsUnknownModesAndOutOfRangeValues() {
    assertThatThrownBy(() -> new GatewayPolicy.Settings(800, "sometimes", 3, true, false))
        .isInstanceOf(ApiException.class);
    assertThatThrownBy(() -> new GatewayPolicy.Settings(10, "none", 3, true, false))
        .isInstanceOf(ApiException.class);
  }
}
