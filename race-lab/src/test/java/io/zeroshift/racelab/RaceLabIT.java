package io.zeroshift.racelab;

import static io.zeroshift.racelab.domain.EventType.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.zeroshift.racelab.application.RunComparison;
import io.zeroshift.racelab.domain.Isolation;
import io.zeroshift.racelab.domain.Mode;
import io.zeroshift.racelab.domain.RaceLabErrors;
import io.zeroshift.racelab.domain.Run;
import io.zeroshift.racelab.domain.RunResult.Outcome;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Every experiment on a real PostgreSQL. With CONTROLLED interleaving the dangerous schedule is
 * forced on every run, so each assertion is deterministic: the unsafe strategy must break the
 * invariant, and each fix must keep it through PostgreSQL's own locks, versions or aborts.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RaceLabIT {
  static RaceLabHarness lab;

  @BeforeAll
  static void start() {
    lab = new RaceLabHarness();
  }

  @AfterAll
  static void stop() {
    lab.close();
  }

  static long number(Map<String, Object> state, String key) {
    return ((Number) state.get(key)).longValue();
  }

  static boolean holds(Run run) {
    return run.result().invariant().holds();
  }

  // ---- 1 overselling ---------------------------------------------------------------------------

  @Test
  void unsafeReservationsOversellTheLastItem() {
    var run = lab.run("oversell", Mode.UNSAFE);
    var result = run.result();
    assertThat(holds(run)).isFalse();
    assertThat(result.metrics().succeeded()).isEqualTo(2);
    assertThat(number(result.finalState(), "stock")).isZero();
    assertThat(number(result.finalState(), "reservations")).isEqualTo(2);
    var reads = lab.events(run, READ_PERFORMED);
    assertThat(reads).extracting(e -> e.valueRead()).containsExactly("stock=1", "stock=1");
    assertThat(result.highlights()).anyMatch(h -> h.kind().equals("SAME_VALUE_READ"));
    assertThat(result.highlights()).anyMatch(h -> h.kind().equals("STALE_WRITE"));
    assertThat(result.explanation().fix().mode()).isEqualTo(Mode.ATOMIC);
    // Every event carries PostgreSQL's own identities for the transaction.
    assertThat(lab.events(run, TRANSACTION_STARTED))
        .allSatisfy(e -> assertThat(e.txId()).isPositive())
        .extracting(e -> e.pid())
        .doesNotHaveDuplicates();
  }

  @Test
  void anAtomicConditionalUpdateSellsTheLastItemOnce() {
    var run = lab.run("oversell", Mode.ATOMIC);
    assertThat(holds(run)).isTrue();
    assertThat(run.result().metrics().succeeded()).isEqualTo(1);
    assertThat(run.result().metrics().rejected()).isEqualTo(1);
    // B's UPDATE waited for A's row lock, then matched no row.
    assertThat(lab.events(run, TRANSACTION_BLOCKED))
        .singleElement()
        .satisfies(
            e -> {
              assertThat(e.lane()).isEqualTo("B");
              assertThat(e.blockedBy()).containsExactly("A");
            });
    assertThat(lab.events(run, WRITE_PERFORMED))
        .anyMatch(e -> e.lane().equals("B") && e.rows() == 0);
  }

  @Test
  void aPessimisticLockMakesTheSecondReaderWait() {
    var run = lab.run("oversell", Mode.PESSIMISTIC);
    assertThat(holds(run)).isTrue();
    assertThat(lab.events(run, TRANSACTION_BLOCKED))
        .singleElement()
        .satisfies(e -> assertThat(e.blockedBy()).containsExactly("A"));
    var bRead =
        lab.events(run, READ_PERFORMED).stream()
            .filter(e -> e.lane().equals("B"))
            .findFirst()
            .orElseThrow();
    assertThat(bRead.valueRead()).isEqualTo("stock=0");
    assertThat(lab.events(run, LOCK_ACQUIRED))
        .anyMatch(e -> e.lane().equals("B") && e.waitMicros() > 0);
  }

  @Test
  void optimisticLockingRejectsTheStaleWriter() {
    var run = lab.run("oversell", Mode.OPTIMISTIC);
    assertThat(holds(run)).isTrue();
    assertThat(lab.events(run, VERSION_CONFLICT))
        .singleElement()
        .satisfies(
            e -> {
              assertThat(e.lane()).isEqualTo("B");
              assertThat(e.expectedVersion()).isEqualTo(1L);
              assertThat(e.actualVersion()).isEqualTo(2L);
            });
    assertThat(lab.events(run, RETRY_SCHEDULED)).hasSize(1);
    var b = run.result().requests().get(1);
    assertThat(b.outcome()).isEqualTo(Outcome.REJECTED);
    assertThat(b.attempts()).isEqualTo(2);
  }

  @Test
  void serializableAbortsTheSecondWriterWhichRetries() {
    var run = lab.run("oversell", Mode.SERIALIZABLE);
    assertThat(run.config().isolation()).isEqualTo(Isolation.SERIALIZABLE);
    assertThat(holds(run)).isTrue();
    assertThat(lab.events(run, SERIALIZATION_FAILURE))
        .singleElement()
        .satisfies(e -> assertThat(e.sqlState()).isEqualTo("40001"));
    assertThat(run.result().metrics().retries()).isEqualTo(1);
    assertThat(run.result().metrics().succeeded()).isEqualTo(1);
  }

  @Test
  void tenRequestsForThreeItemsOversellOnlyWhenUnsafe() {
    var unsafe = lab.run("oversell", Mode.UNSAFE, null, 10, 3, 20, -1);
    assertThat(holds(unsafe)).isFalse();
    assertThat(number(unsafe.result().finalState(), "reservations")).isEqualTo(10);
    var atomic = lab.run("oversell", Mode.ATOMIC, null, 10, 3, 20, -1);
    assertThat(holds(atomic)).isTrue();
    assertThat(atomic.result().metrics().succeeded()).isEqualTo(3);
    assertThat(atomic.result().metrics().rejected()).isEqualTo(7);
  }

  // ---- 2–6 -------------------------------------------------------------------------------------

  @Test
  void aReadModifyWriteLosesADepositAndAnAtomicIncrementDoesNot() {
    var unsafe = lab.run("lost-update", Mode.UNSAFE);
    assertThat(holds(unsafe)).isFalse();
    assertThat(number(unsafe.result().finalState(), "balance")).isEqualTo(110);
    assertThat(number(unsafe.result().finalState(), "deposits")).isEqualTo(2);
    for (var mode :
        new Mode[] {Mode.ATOMIC, Mode.PESSIMISTIC, Mode.OPTIMISTIC, Mode.SERIALIZABLE}) {
      var run = lab.run("lost-update", mode);
      assertThat(holds(run)).as(mode.name()).isTrue();
      assertThat(number(run.result().finalState(), "balance")).as(mode.name()).isEqualTo(120);
    }
  }

  @Test
  void checkThenActChargesTwiceAndAClaimChargesOnce() {
    var unsafe = lab.run("double-payment", Mode.UNSAFE);
    assertThat(holds(unsafe)).isFalse();
    assertThat(number(unsafe.result().finalState(), "payments")).isEqualTo(2);
    var claim = lab.run("double-payment", Mode.ATOMIC);
    assertThat(holds(claim)).isTrue();
    assertThat(number(claim.result().finalState(), "payments")).isEqualTo(1);
  }

  @Test
  void incompatibleTransitionsBothApplyUnlessTheyCompareAndSet() {
    var unsafe = lab.run("state-transition", Mode.UNSAFE);
    assertThat(holds(unsafe)).isFalse();
    assertThat(number(unsafe.result().finalState(), "shipments")).isEqualTo(1);
    assertThat(number(unsafe.result().finalState(), "refunds")).isEqualTo(1);
    for (var mode : new Mode[] {Mode.ATOMIC, Mode.PESSIMISTIC, Mode.OPTIMISTIC, Mode.SERIALIZABLE})
      assertThat(holds(lab.run("state-transition", mode))).as(mode.name()).isTrue();
  }

  @Test
  void theVersionColumnTurnsASilentOverwriteIntoARejection() {
    var unsafe = lab.run("optimistic-conflict", Mode.UNSAFE);
    assertThat(holds(unsafe)).isFalse();
    assertThat(number(unsafe.result().finalState(), "version")).isEqualTo(5);
    var optimistic = lab.run("optimistic-conflict", Mode.OPTIMISTIC);
    assertThat(holds(optimistic)).isTrue();
    assertThat(lab.events(optimistic, VERSION_CONFLICT))
        .singleElement()
        .satisfies(
            e -> {
              assertThat(e.expectedVersion()).isEqualTo(4L);
              assertThat(e.actualVersion()).isEqualTo(5L);
            });
    assertThat(optimistic.result().requests().get(1).outcome()).isEqualTo(Outcome.ABORTED);
    assertThat(optimistic.result().highlights())
        .anyMatch(
            h ->
                h.title()
                    .equals(
                        "Optimistic lock rejected B because expected version=4, actual version=5"));
  }

  @Test
  void aPessimisticLockQueuesEveryRequestBehindTheOnesBeforeIt() {
    var run = lab.run("pessimistic-locking", Mode.PESSIMISTIC);
    assertThat(holds(run)).isTrue();
    assertThat(run.result().metrics().succeeded()).isEqualTo(5);
    // B, C, D and E each blocked; each waited at least as long as A held the lock (200 ms).
    var blocked =
        lab.events(run, TRANSACTION_BLOCKED).stream().map(e -> e.lane()).distinct().toList();
    assertThat(blocked).containsExactly("B", "C", "D", "E");
    assertThat(run.result().requests().get(4).lockWaitMicros()).isGreaterThan(4 * 150_000L);
    // Each request read the stock only after the one before it committed: nothing it wrote was
    // stale.
    assertThat(run.result().highlights()).noneMatch(h -> h.kind().equals("STALE_WRITE"));
    var atomic = lab.run("pessimistic-locking", Mode.ATOMIC);
    assertThat(holds(atomic)).isTrue();
    assertThat(atomic.result().metrics().lockWaitMicros())
        .isLessThan(run.result().metrics().lockWaitMicros());
  }

  // ---- 7 deadlock ------------------------------------------------------------------------------

  @Test
  void oppositeLockOrderDeadlocksAndPostgresAbortsOneVictim() {
    var run = lab.run("deadlock", Mode.UNSAFE);
    assertThat(holds(run)).isFalse();
    var deadlock = lab.events(run, DEADLOCK_DETECTED);
    assertThat(deadlock)
        .singleElement()
        .satisfies(
            e -> {
              assertThat(e.sqlState()).isEqualTo("40P01");
              assertThat(e.blockedBy()).hasSize(1);
              assertThat(String.valueOf(e.data().get("postgresDetail"))).contains("waits for");
            });
    // Before PostgreSQL broke the cycle, each transaction was blocked by the other.
    assertThat(lab.events(run, TRANSACTION_BLOCKED))
        .extracting(e -> e.lane() + "<" + e.blockedBy().getFirst())
        .contains("A<B", "B<A");
    assertThat(run.result().metrics().succeeded()).isEqualTo(1);
    assertThat(number(run.result().finalState(), "total")).isEqualTo(200);
    var ordered = lab.run("deadlock", Mode.ORDERED_LOCKS);
    assertThat(holds(ordered)).isTrue();
    assertThat(lab.events(ordered, DEADLOCK_DETECTED)).isEmpty();
  }

  // ---- 8–10 isolation anomalies ----------------------------------------------------------------

  @Test
  void readCommittedAllowsANonRepeatableReadAndRepeatableReadDoesNot() {
    var rc = lab.run("non-repeatable-read", Mode.UNSAFE, Isolation.READ_COMMITTED, 2, 100, 50, -1);
    assertThat(holds(rc)).isFalse();
    assertThat(lab.events(rc, READ_PERFORMED).stream().filter(e -> e.lane().equals("A")))
        .extracting(e -> e.valueRead())
        .containsExactly("balance=100", "balance=150");
    assertThat(rc.result().highlights()).anyMatch(h -> h.kind().equals("CHANGED_UNDER_READER"));
    var rr = lab.run("non-repeatable-read", Mode.UNSAFE, Isolation.REPEATABLE_READ, 2, 100, 50, -1);
    assertThat(holds(rr)).isTrue();
  }

  @Test
  void aPhantomAppearsAtReadCommittedButNotInPostgresRepeatableRead() {
    var rc = lab.run("phantom-read", Mode.UNSAFE, Isolation.READ_COMMITTED, 2, 3, 50, -1);
    assertThat(holds(rc)).isFalse();
    var rr = lab.run("phantom-read", Mode.UNSAFE, Isolation.REPEATABLE_READ, 2, 3, 50, -1);
    assertThat(holds(rr)).isTrue();
  }

  @Test
  void writeSkewSurvivesSnapshotIsolationButNotSerializable() {
    var rr = lab.run("write-skew", Mode.UNSAFE);
    assertThat(rr.config().isolation()).isEqualTo(Isolation.REPEATABLE_READ);
    assertThat(holds(rr)).isFalse();
    assertThat(number(rr.result().finalState(), "on_call")).isZero();
    assertThat(holds(lab.run("write-skew", Mode.ATOMIC))).isFalse();
    var serializable = lab.run("write-skew", Mode.SERIALIZABLE);
    assertThat(holds(serializable)).isTrue();
    assertThat(lab.events(serializable, SERIALIZATION_FAILURE)).hasSize(1);
    assertThat(holds(lab.run("write-skew", Mode.PESSIMISTIC))).isTrue();
  }

  // ---- lab behaviour ---------------------------------------------------------------------------

  @Test
  void comparingTwoRunsMeasuresWhatTheFixCost() {
    var unsafe = lab.run("oversell", Mode.UNSAFE);
    var pessimistic = lab.run("oversell", Mode.PESSIMISTIC);
    var comparison = new RunComparison(lab.runs).compare(unsafe.id(), pessimistic.id());
    assertThat(comparison.left().sequence()).contains("A READ stock=1", "B READ stock=1");
    assertThat(comparison.right().sequence()).contains("B WAITS for A", "B READ stock=0");
    assertThat(comparison.differences())
        .filteredOn(d -> d.metric().equals("Invariant held"))
        .singleElement()
        .satisfies(d -> assertThat(d.better()).isEqualTo("right"));
    assertThat(comparison.verdict()).startsWith("Pessimistic lock kept the invariant");
  }

  @Test
  void aSlowLiveListenerNeverSlowsTheTransactionsItWatches() {
    var slow =
        new io.zeroshift.racelab.application.RunStreams.Listener() {
          @Override
          public void event(io.zeroshift.racelab.domain.RaceEvent event) {
            RaceLabHarness.sleep(100);
          }

          @Override
          public void finished(Run run) {}
        };
    var run = lab.run("oversell", Mode.ATOMIC, null, 2, 1, 50, -1, slow);
    assertThat(holds(run)).isTrue();
    // ~30 events × 100 ms would add seconds if delivery ran on the transaction threads.
    assertThat(run.result().metrics().durationMicros()).isLessThan(1_500_000L);
    var blocked = lab.events(run, TRANSACTION_BLOCKED).getFirst();
    var requested =
        lab.events(run, LOCK_REQUESTED).stream()
            .filter(e -> e.lane().equals("B"))
            .findFirst()
            .orElseThrow();
    assertThat(blocked.atMicros() - requested.atMicros()).isLessThan(200_000L);
  }

  @Test
  void anUnsupportedModeIsRefused() {
    assertThatThrownBy(() -> lab.run("deadlock", Mode.OPTIMISTIC))
        .isInstanceOf(RaceLabErrors.InvalidConfig.class);
  }

  @Test
  @Order(Integer.MAX_VALUE)
  void resetRestoresTheStartingStateOfTheFirstRun() {
    var before = lab.run("oversell", Mode.UNSAFE);
    var deleted = lab.engine.reset();
    assertThat(deleted.get("run")).isPositive();
    assertThat(deleted.get("product")).isPositive();
    assertThat(lab.runs.recent(null, 10)).isEmpty();
    var first = lab.run("oversell", Mode.UNSAFE);
    assertThat(first.id()).isEqualTo(1);
    assertThat(first.result().initialState()).isEqualTo(before.result().initialState());
    assertThat(first.result().invariant().holds()).isFalse();
  }
}
