package io.zeroshift.racelab.experiments;

import io.zeroshift.racelab.application.Experiment;
import io.zeroshift.racelab.application.Participant;
import io.zeroshift.racelab.application.Participant.Read;
import io.zeroshift.racelab.application.Participant.Write;
import io.zeroshift.racelab.domain.EventType;
import io.zeroshift.racelab.domain.Isolation;
import io.zeroshift.racelab.domain.Mode;
import io.zeroshift.racelab.domain.RaceEvent;
import io.zeroshift.racelab.domain.RunConfig;
import io.zeroshift.racelab.domain.RunResult.Explanation;
import io.zeroshift.racelab.domain.RunResult.InvariantResult;
import io.zeroshift.racelab.domain.RunResult.RequestResult;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Isolation anomalies seen by one reader: it reads the same thing twice inside one transaction
 * while a writer commits a change between the two reads. What the second read returns depends only
 * on the reader's isolation level.
 */
abstract class ReadTwice implements Experiment {
  abstract Read query(Participant p);

  abstract Write change(Participant p);

  abstract String changed();

  @Override
  public void attempt(Participant p) {
    p.begin();
    if ("reader".equals(p.role())) {
      var first = p.read(query(p));
      p.sync("first read");
      p.think();
      p.sync("writer committed");
      var second = p.read(query(p));
      var a = first.values().get(query(p).value());
      var b = second.values().get(query(p).value());
      boolean same = Objects.equals(a, b);
      p.decide(
          same,
          "read 1 = read 2",
          same
              ? "one consistent view for the whole transaction"
              : "the data changed under the transaction");
      p.commit("read " + a + ", then " + b);
    } else {
      p.sync("first read");
      p.write(change(p));
      p.commit(changed());
      p.sync("writer committed");
    }
  }

  @Override
  public boolean retries(Mode mode) {
    return false;
  }

  @Override
  public InvariantResult check(
      RunContext run,
      Map<String, Object> initial,
      Map<String, Object> result,
      List<RequestResult> requests,
      List<RaceEvent> events) {
    var reads =
        events.stream()
            .filter(e -> e.type() == EventType.READ_PERFORMED && "A".equals(e.lane()))
            .map(RaceEvent::valueRead)
            .toList();
    boolean holds = reads.stream().distinct().count() <= 1;
    return new InvariantResult(
        info().invariant(),
        holds,
        "both reads return the same value",
        "read " + String.join(", then ", reads) + " at " + run.config().isolation().sql(),
        holds
            ? "The reader saw one snapshot, although the writer committed in between."
            : "The writer's commit became visible between the reader's two statements.");
  }

  @Override
  public String conclusion(
      RunContext run, InvariantResult invariant, List<RequestResult> requests) {
    var iso = run.config().isolation();
    return invariant.holds()
        ? "At "
            + iso.sql()
            + " the reader's snapshot was taken once, at the start of its transaction. The"
            + " writer's commit happened, but not in the reader's world: every statement of the"
            + " transaction saw the database as of BEGIN."
        : "At READ COMMITTED every statement takes a fresh snapshot of what is committed at that"
            + " moment. The writer committed between the reader's two statements, so the second"
            + " statement saw it. Each read was correct; together they describe two different"
            + " databases.";
  }

  @Override
  public Explanation.Fix fix(RunConfig config, InvariantResult invariant) {
    if (invariant.holds()) return null;
    return Texts.fix(
        Mode.UNSAFE,
        Isolation.REPEATABLE_READ,
        "REPEATABLE READ takes one snapshot per transaction instead of one per statement.");
  }
}
