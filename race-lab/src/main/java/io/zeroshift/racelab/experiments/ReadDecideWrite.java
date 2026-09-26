package io.zeroshift.racelab.experiments;

import io.zeroshift.racelab.application.Experiment;
import io.zeroshift.racelab.application.Participant;
import io.zeroshift.racelab.application.Participant.Read;
import io.zeroshift.racelab.application.Participant.Row;
import io.zeroshift.racelab.application.Participant.Write;
import io.zeroshift.racelab.domain.Mode;

/**
 * The shape most races share: read a row, decide in the application, do some work, write back.
 * Each strategy changes one thing:
 *
 * <ul>
 *   <li>UNSAFE and SERIALIZABLE run the same code; SERIALIZABLE only changes the isolation level.
 *   <li>PESSIMISTIC reads with {@code FOR UPDATE}, so the next request's read waits for the lock.
 *   <li>OPTIMISTIC writes {@code WHERE version = :read}; no row matched means someone was first.
 *   <li>ATOMIC skips the application's check: the conditional UPDATE decides, on the locked row.
 * </ul>
 *
 * <p>With CONTROLLED interleaving, requests read in turn, meet before anyone decides, write in
 * turn, and meet again before committing. That is the interleaving that exposes the bug, forced on
 * every run; a request blocked by PostgreSQL counts as arrived, so safe strategies still get their
 * waits.
 */
abstract class ReadDecideWrite implements Experiment {
  /** The application's check on the row it read; {@code rejection} is its answer when it fails. */
  record Decision(boolean ok, String rule, String because, String rejection) {}

  /** The shared row: "item #3". */
  abstract String target(Participant p);

  /** The SELECT of the shared row, with {@code FOR UPDATE} when {@code lock}. */
  abstract Read read(Participant p, boolean lock);

  /** Null when requests do not check anything before writing (a deposit). */
  abstract Decision decide(Participant p, Row row);

  /** UNSAFE, PESSIMISTIC, SERIALIZABLE: write back a value computed from the read. */
  abstract Write blindWrite(Participant p, Row row);

  /** ATOMIC: one UPDATE whose WHERE clause re-checks the condition. */
  abstract Write conditionalWrite(Participant p, Row row);

  /** OPTIMISTIC: the blind write, only if the version is still the one read. */
  abstract Write versionedWrite(Participant p, Row row);

  /** Why a conditional UPDATE that matched no row means the request must decline. */
  abstract String conditionFailed(Participant p);

  /** What the request adds once its write succeeded (a reservation, a charge); may be null. */
  abstract Write record(Participant p, Row row);

  abstract String succeeded(Participant p, Row row);

  @Override
  public void attempt(Participant p) {
    p.begin();
    boolean lock = p.mode() == Mode.PESSIMISTIC;
    Row row =
        p.inTurn(
            "read",
            () -> lock ? p.lockRead(read(p, true), "row lock on " + target(p)) : p.read(read(p, false)));
    p.sync("read");
    if (p.mode() != Mode.ATOMIC) {
      var decision = decide(p, row);
      if (decision != null) {
        p.decide(decision.ok(), decision.rule(), decision.because());
        if (!decision.ok()) {
          p.reject(decision.rejection());
          return;
        }
      }
    }
    p.think();
    int rows =
        p.inTurn(
            "write",
            () ->
                p.write(
                    switch (p.mode()) {
                      case ATOMIC -> conditionalWrite(p, row);
                      case OPTIMISTIC -> versionedWrite(p, row);
                      default -> blindWrite(p, row);
                    }));
    if (rows == 0) {
      if (p.mode() == Mode.OPTIMISTIC)
        throw p.versionConflict(target(p), row.number("version"), read(p, false));
      p.sync("written");
      p.reject(conditionFailed(p));
      return;
    }
    var extra = record(p, row);
    if (extra != null) p.write(extra);
    p.sync("written");
    p.commit(succeeded(p, row));
  }
}
