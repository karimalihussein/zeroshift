package io.zeroshift.domain;

import static org.assertj.core.api.Assertions.*;

import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

class DomainTest {
  @Test
  void upsertsRequireMatchingTypedPayload() {
    assertThatThrownBy(() -> new Change(1, Change.Operation.UPDATE, null, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new Change(2, Change.Operation.UPDATE, new Row.Customer(1, "a", null, true), 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(new Change(1, Change.Operation.DELETE, null, 1).row()).isNull();
  }

  @Test
  void completedAndIdleRunsCannotBeResumedOrPaused() {
    assertThat(Stage.IDLE.canPause()).isFalse();
    assertThat(Stage.COMPLETE.canPause()).isFalse();
    assertThat(Stage.FREEZE.canPause()).isTrue();
  }

  @Test
  void trafficMixIsReadHeavyAndCoversEveryOperation() {
    var mix = LongStream.range(0, 10).mapToObj(TrafficOperation::forStep).toList();
    assertThat(mix).filteredOn(o -> o == TrafficOperation.READ).hasSize(4);
    assertThat(mix).containsOnlyOnce(TrafficOperation.DELETE);
    assertThat(mix).filteredOn(o -> o == TrafficOperation.UPDATE).hasSize(3);
    assertThat(mix).filteredOn(o -> o == TrafficOperation.INSERT).hasSize(2);
    assertThat(TrafficOperation.forStep(10)).isEqualTo(TrafficOperation.forStep(0));
  }
}
