package io.zeroshift.failures;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.zeroshift.kafkalab.KafkaLabRuns;
import io.zeroshift.platform.web.ApiException;
import io.zeroshift.racelab.infrastructure.OtelTracing;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

class FailureLabsTest {
  static final JsonMapper JSON = JsonMapper.builder().build();

  /** A lab whose stage 2 claim holds only on the second try. */
  static class Fake implements FailureLab {
    final AtomicInteger attempts = new AtomicInteger();
    volatile CountDownLatch hold;

    @Override
    public String id() {
      return "two-phase";
    }

    @Override
    public String title() {
      return "Fake";
    }

    @Override
    public String summary() {
      return "";
    }

    @Override
    public String naive() {
      return "";
    }

    @Override
    public String correct() {
      return "";
    }

    @Override
    public List<String> plan() {
      return List.of("a", "b", "c", "d", "e");
    }

    @Override
    public List<String> requires() {
      return List.of();
    }

    @Override
    public ObjectNode run(int stage, ObjectNode memo, Trace trace) throws Exception {
      if (hold != null) hold.await();
      var result = JSON.createObjectNode();
      memo.put("seen" + stage, true);
      if (stage == 2) Checks.check(result, "second try holds", 2, attempts.incrementAndGet());
      return result;
    }

    @Override
    public ObjectNode state() {
      return JSON.createObjectNode();
    }

    @Override
    public ObjectNode reset() {
      return JSON.createObjectNode();
    }
  }

  final Fake lab = new Fake();
  final KafkaLabRuns stored = mock(KafkaLabRuns.class);
  final FailureLabs labs =
      new FailureLabs(List.of(lab), stored, new OtelTracing(), new SimpleMeterRegistry());

  @Test
  void stagesRunInOrder() {
    assertThatThrownBy(() -> labs.stage("two-phase", 1))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("stage 1 is next");
    labs.stage("two-phase", 0);
    assertThatThrownBy(() -> labs.stage("two-phase", 2)).hasMessageContaining("stage 2 is next");
  }

  @Test
  void aClaimThatDoesNotHoldIsKeptAsTheLastFailureNotAsDone() {
    labs.stage("two-phase", 0);
    labs.stage("two-phase", 1);
    assertThatThrownBy(() -> labs.stage("two-phase", 2))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("second try holds: expected 2, observed 1");
    var run = labs.all().getFirst().run();
    assertThat(run.stages()).hasSize(2);
    assertThat(run.lastFailure().result().path("checks").get(0).path("observed").asString())
        .isEqualTo("1");

    var retried = labs.stage("two-phase", 2);
    assertThat(retried.stages()).hasSize(3);
    assertThat(retried.lastFailure()).isNull();
  }

  @Test
  void aCompleteRunIsStoredOnce() {
    for (int n = 0; n < 2; n++) labs.stage("two-phase", n);
    assertThatThrownBy(() -> labs.stage("two-phase", 2));
    for (int n = 2; n < 5; n++) labs.stage("two-phase", n);
    verify(stored).record(eq(FailureLabs.LAB), eq("two-phase"), any(), any());
  }

  @Test
  void oneStageAtATimePerLab() throws Exception {
    lab.hold = new CountDownLatch(1);
    var first = Thread.ofVirtual().start(() -> labs.stage("two-phase", 0));
    while (!labs.all().getFirst().busy()) Thread.sleep(5);
    assertThatThrownBy(() -> labs.reset("two-phase")).hasMessageContaining("still running");
    lab.hold.countDown();
    first.join();
    labs.reset("two-phase");
    assertThat(labs.all().getFirst().run()).isNull();
    verify(stored, never()).record(any(), any(), any(), any());
  }

  @Test
  void unknownLab() {
    assertThatThrownBy(() -> labs.stage("nope", 0)).hasMessageContaining("No failure lab nope");
  }
}
