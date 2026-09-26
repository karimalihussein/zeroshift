package io.zeroshift.platform;

import static org.assertj.core.api.Assertions.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class LabPressureTest {
  @Test
  void readsPoolDecisionsAndLabCountersFromTheRegistry() {
    var meters = new SimpleMeterRegistry();
    var pending = new AtomicInteger(3);
    meters.gauge("hikaricp.connections.pending", pending);
    // Gauges hold their value weakly: keep a strong reference, or GC can turn it into 0.
    var max = new AtomicInteger(10);
    meters.gauge("hikaricp.connections.max", max);
    meters
        .counter("zeroshift.consumer.decisions", "consumer", "a", "decision", "PROCESSED")
        .increment(4);
    meters
        .counter("zeroshift.consumer.decisions", "consumer", "b", "decision", "PROCESSED")
        .increment(2);
    meters
        .counter("zeroshift.consumer.decisions", "consumer", "a", "decision", "DEAD_LETTERED")
        .increment();
    meters.counter("zeroshift.gateway.attempts", "outcome", "TIMEOUT").increment(7);
    meters.counter("kafka.consumer.coordinator.rebalance.total", "client.id", "x").increment(2);
    meters.counter("unrelated.meter").increment();

    var p = new LabPressure(meters, "payment-service", "payment-1").read();

    assertThat(p.pool().pending()).isEqualTo(3);
    assertThat(p.pool().max()).isEqualTo(max.get());
    assertThat(p.decisions()).containsEntry("PROCESSED", 6.0).containsEntry("DEAD_LETTERED", 1.0);
    assertThat(p.counters())
        .containsEntry("zeroshift.gateway.attempts{outcome=TIMEOUT}", 7.0)
        .doesNotContainKey("unrelated.meter");
    assertThat(p.rebalances()).isEqualTo(2.0);
    assertThat(p.service()).isEqualTo("payment-service");
  }
}
