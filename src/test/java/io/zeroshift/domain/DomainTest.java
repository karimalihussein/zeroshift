package io.zeroshift.domain;

import static org.assertj.core.api.Assertions.*;

import java.time.Instant;
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
    assertThat(Stage.COMPLETED.canPause()).isFalse();
    assertThat(Stage.FREEZE.canPause()).isFalse();
    assertThat(Stage.FREEZE.canResume()).isTrue();
  }

  @Test
  void rollbackHoldsWritesFromTheFreezeAndCanBeAbandonedOnlyBeforeTheSwitch() {
    assertThat(Stage.values())
        .filteredOn(Stage::writesHeld)
        .containsExactly(
            Stage.FREEZE,
            Stage.VALIDATION,
            Stage.CUTOVER,
            Stage.ROLLBACK_FREEZE,
            Stage.FINAL_SYNC,
            Stage.SWITCH_PRIMARY);
    assertThat(Stage.values())
        .filteredOn(Stage::rollbackAbortable)
        .containsExactly(
            Stage.ROLLBACK_PREPARE,
            Stage.REVERSE_CATCH_UP,
            Stage.ROLLBACK_VALIDATION,
            Stage.ROLLBACK_FREEZE,
            Stage.FINAL_SYNC);
    assertThat(Stage.ROLLED_BACK.canResume()).isFalse();
    assertThat(Stage.FINAL_SYNC.canPause()).isFalse();
    assertThat(Stage.REVERSE_CATCH_UP.canPause()).isTrue();
    // The SQL Server fence rule for cutover must not leak into rollback stages.
    assertThat(Stage.FINAL_SYNC.sourceMustRemainFrozen()).isFalse();
  }

  @Test
  void snapshotWithNoExpectedWorkNeverReportsProgress() {
    var state =
        new MigrationState(
            Stage.SNAPSHOT,
            RunStatus.RUNNING,
            Primary.SQL_SERVER,
            Table.CUSTOMERS,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            0,
            false,
            "Not checked",
            "",
            Instant.now(),
            0,
            false,
            false,
            null,
            null,
            null,
            0);

    assertThat(state.progress()).isZero();
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
