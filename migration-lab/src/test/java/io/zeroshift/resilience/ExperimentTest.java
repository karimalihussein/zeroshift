package io.zeroshift.resilience;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.zeroshift.resilience.Experiment.Phase;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExperimentTest {
  private final ExperimentCatalog catalog =
      new ExperimentCatalog(mock(Levers.class), mock(LoadGenerator.class), mock(Sampler.class));

  @Test
  void everyExperimentFollowsTheSevenStepsInOrder() {
    assertThat(catalog.all()).hasSize(7);
    for (var e : catalog.all()) {
      assertThat(e.steps()).extracting(Experiment.Step::phase).containsExactly(Phase.values());
      var byPhase = new EnumMap<Phase, Experiment.Step>(Phase.class);
      e.steps().forEach(s -> byPhase.put(s.phase(), s));
      assertThat(byPhase.get(Phase.INJECT).action()).as(e.id() + " injects").isNotNull();
      assertThat(byPhase.get(Phase.MITIGATE).action()).as(e.id() + " mitigates").isNotNull();
      assertThat(byPhase.get(Phase.RECOVER).action()).as(e.id() + " recovers").isNotNull();
      assertThat(byPhase.get(Phase.OBSERVE).checks()).as(e.id() + " observes").isNotEmpty();
      assertThat(byPhase.get(Phase.MITIGATE).checks())
          .as(e.id() + " checks the mitigation")
          .isNotEmpty();
      assertThat(byPhase.get(Phase.VERIFY).checks()).as(e.id() + " verifies").isNotEmpty();
    }
  }

  @Test
  void networkExperimentsAreTheOnesThatNeedToxiproxy() {
    assertThat(catalog.all().stream().filter(Experiment::needsChaos).map(Experiment::id))
        .containsExactlyInAnyOrder("bulkhead", "retry-storm", "kafka-partition", "cdc-degraded");
  }

  @Test
  void aWindowReadsThisStepAndTheBaseline() {
    var baseline = List.of(sample(0, 10), sample(1, 20));
    var current = List.of(sample(2, 50), sample(3, 70), sample(4, 90));
    var window =
        new Experiment.Window(current, Map.of(Phase.HYPOTHESIS, baseline, Phase.OBSERVE, current));
    Experiment.Metric lag = Metrics.lag("payment-service");

    assertThat(window.baseline(lag)).isEqualTo(15.0);
    assertThat(window.latest(lag)).isEqualTo(90.0);
    assertThat(window.recent(lag, 2)).isEqualTo(80.0);
    assertThat(window.ago(lag, 2)).isEqualTo(50.0);
    assertThat(window.max(lag)).isEqualTo(90.0);
    assertThat(window.peak(lag, Phase.HYPOTHESIS)).isEqualTo(20.0);
    assertThat(window.seconds()).isEqualTo(3);
  }

  @Test
  void anUnmeasuredValueNeverHolds() {
    assertThat(ExperimentCatalog.above(null, 0, "%.0f").holds()).isFalse();
    assertThat(ExperimentCatalog.ratio(3.0, 0.0, 0.5, "%.0f").holds()).isFalse();
    assertThat(ExperimentCatalog.ratio(9.0, 10.0, 0.8, "%.0f %%").observed()).isEqualTo("90 %");
    assertThat(ExperimentCatalog.below(5.0, 5, "%.0f").holds()).isTrue();
  }

  static Sample sample(long t, long paymentLag) {
    return new Sample(
        t * 1000,
        new Sample.Traffic(5, 0, 5, 0, 5, 0, 0, Map.of("OK", 5), new int[10], 0),
        new Sample.Latency(10, 20, 30, null, null, null),
        new Sample.Edge(5.0, 0.0, 0.0, 0.0, 1),
        new Sample.Sagas(5.0, 0.0, 10L, 100.0, 200.0, 300.0),
        Map.of("payment-service", paymentLag),
        new Sample.Consumers(10.0, 0.0, 0.0, 0.0, 0.0),
        new Sample.Gateway(Map.of("CHARGED", 5.0), 5.0, "CLOSED", false, 1500, "exponential"),
        new Sample.Relay(10, 400L, 500L),
        Map.of(),
        List.of());
  }
}
