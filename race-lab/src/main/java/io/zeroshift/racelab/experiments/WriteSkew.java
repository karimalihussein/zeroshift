package io.zeroshift.racelab.experiments;

import static io.zeroshift.racelab.experiments.Texts.number;

import io.zeroshift.racelab.application.Experiment;
import io.zeroshift.racelab.application.Participant;
import io.zeroshift.racelab.application.Participant.Read;
import io.zeroshift.racelab.application.Participant.Row;
import io.zeroshift.racelab.application.Participant.Write;
import io.zeroshift.racelab.application.port.LabDatabase;
import io.zeroshift.racelab.domain.ExperimentInfo;
import io.zeroshift.racelab.domain.ExperimentInfo.ModeInfo;
import io.zeroshift.racelab.domain.Isolation;
import io.zeroshift.racelab.domain.Mode;
import io.zeroshift.racelab.domain.RaceEvent;
import io.zeroshift.racelab.domain.RunConfig;
import io.zeroshift.racelab.domain.RunResult.Explanation;
import io.zeroshift.racelab.domain.RunResult.InvariantResult;
import io.zeroshift.racelab.domain.RunResult.RequestResult;
import java.util.List;
import java.util.Map;

/**
 * 10. Write skew: two doctors each check that someone else is on call, then each takes themselves
 * off. They write different rows, so no lock ever conflicts, and snapshot isolation lets both
 * commit. Only SERIALIZABLE (or locking every row the decision read) sees the conflict.
 */
public class WriteSkew implements Experiment {
  static final List<String> DOCTORS = List.of("Dr. Alice", "Dr. Bob", "Dr. Chen", "Dr. Diaz", "Dr. Evans");
  static final String COUNT =
      "SELECT count(*) AS on_call FROM doctor WHERE run_id = ? AND shift = 'night' AND on_call";
  static final String COUNT_LOCKED =
      "SELECT count(*) AS on_call FROM (SELECT id FROM doctor WHERE run_id = ? AND shift = 'night'"
          + " AND on_call FOR UPDATE) AS locked";

  @Override
  public ExperimentInfo info() {
    return new ExperimentInfo(
        "write-skew",
        10,
        "Write skew",
        "Isolation anomalies",
        "The night shift needs at least one doctor on call. Two on-call doctors both ask to go"
            + " home. Can the ward end up with nobody?",
        "Each doctor's request counts the doctors on call, sees two, decides one may leave, and"
            + " updates only its own row. The two writes touch different rows, so they never wait"
            + " for each other, and at REPEATABLE READ each transaction reads a snapshot that does"
            + " not include the other's change. Both commit; nobody is on call. An atomic UPDATE"
            + " does not help either: it only guards the row it writes. SERIALIZABLE tracks what"
            + " each transaction read and aborts one of them.",
        "at least one doctor stays on call",
        "Doctors on call",
        List.of(
            new ModeInfo(
                Mode.UNSAFE,
                "Snapshot check",
                "Count the on-call doctors, check count ≥ 2 in the application, update your own"
                    + " row. Defaults to REPEATABLE READ to show that snapshot isolation is not"
                    + " enough.",
                "SELECT count(*) FROM doctor WHERE shift = 'night' AND on_call;\n"
                    + "-- if (count >= 2) …\nUPDATE doctor SET on_call = false WHERE id = :me;",
                Isolation.REPEATABLE_READ,
                false),
            new ModeInfo(
                Mode.ATOMIC,
                "Conditional update",
                "Put the count in the UPDATE's WHERE clause. It still breaks: each UPDATE locks only"
                    + " its own row, and the subquery counts rows the other transaction is changing"
                    + " but has not committed.",
                "UPDATE doctor SET on_call = false WHERE id = :me\n"
                    + " AND (SELECT count(*) FROM doctor WHERE shift = 'night' AND on_call) >= 2;",
                Isolation.READ_COMMITTED,
                false),
            new ModeInfo(
                Mode.PESSIMISTIC,
                "Lock every row the decision reads",
                "Lock all on-call doctors with FOR UPDATE while counting. The second request waits;"
                    + " when it resumes, the first doctor's row no longer matches on_call and the"
                    + " count is 1.",
                "SELECT count(*) FROM (SELECT id FROM doctor\n WHERE shift = 'night' AND on_call"
                    + " FOR UPDATE) locked;",
                Isolation.READ_COMMITTED,
                true),
            new ModeInfo(
                Mode.SERIALIZABLE,
                "Serializable",
                "The snapshot check at SERIALIZABLE. PostgreSQL's serializable snapshot isolation"
                    + " notices that each transaction read what the other wrote and aborts one"
                    + " (40001); its retry counts one doctor and stays.",
                "BEGIN ISOLATION LEVEL SERIALIZABLE;\n-- same code\n-- 40001 → retry",
                Isolation.SERIALIZABLE,
                true)),
        Mode.UNSAFE,
        Isolation.REPEATABLE_READ,
        new ExperimentInfo.Limits(2, 5, 2, 2, 5, 2, 150, 3),
        DOCTORS,
        "A counted 2 on call · B counted 2 on call · A went off · B went off → nobody on call.");
  }

  @Override
  public Map<String, Object> seed(LabDatabase db, RunContext run) {
    int doctors = Math.max(run.config().requests(), run.config().initialValue());
    var keys = new java.util.LinkedHashMap<String, Object>();
    keys.put("run", run.runId());
    for (int i = 0; i < doctors; i++)
      keys.put(
          "doctor" + i,
          db.insert(
              "INSERT INTO doctor(run_id, name, shift, on_call, version) VALUES (?, ?, 'night', true, 1)"
                  + " RETURNING id",
              run.runId(),
              DOCTORS.get(i)));
    return keys;
  }

  @Override
  public void attempt(Participant p) {
    long run = p.key("run");
    long me = p.key("doctor" + p.index());
    var shift = "night shift on call";
    p.begin();
    Row row =
        p.inTurn(
            "read",
            () ->
                p.mode() == Mode.PESSIMISTIC
                    ? p.lockRead(Read.of(shift, COUNT_LOCKED, "on_call", run), "row locks on every on-call doctor")
                    : p.read(Read.of(shift, COUNT, "on_call", run)));
    p.sync("read");
    long onCall = row.number("on_call");
    if (p.mode() != Mode.ATOMIC) {
      boolean ok = onCall >= 2;
      p.decide(ok, "on_call ≥ 2", ok ? onCall + " on call: someone else stays" : "only " + onCall + " on call");
      if (!ok) {
        p.reject(p.role() + " must stay: the last doctor on call");
        return;
      }
    }
    p.think();
    var target = p.role() + " (doctor #" + me + ")";
    int rows =
        p.inTurn(
            "write",
            () ->
                p.write(
                    p.mode() == Mode.ATOMIC
                        ? Write.update(
                            target,
                            "UPDATE doctor SET on_call = false, version = version + 1 WHERE id = ?"
                                + " AND (" + COUNT + ") >= 2",
                            "on_call=false",
                            me,
                            run)
                        : Write.update(
                            target,
                            "UPDATE doctor SET on_call = false, version = version + 1 WHERE id = ?",
                            "on_call=false",
                            me)));
    p.sync("written");
    if (rows == 0) {
      p.reject(p.role() + " must stay: the conditional UPDATE matched no row");
      return;
    }
    p.commit(p.role() + " went off call");
  }

  @Override
  public Map<String, Object> observe(LabDatabase db, RunContext run) {
    return db.one(
        "SELECT count(*) FILTER (WHERE on_call) AS on_call, count(*) AS doctors FROM doctor WHERE run_id = ?",
        run.runId());
  }

  @Override
  public String describe(Map<String, Object> state) {
    return "on call=" + number(state, "on_call") + " of " + number(state, "doctors");
  }

  @Override
  public InvariantResult check(
      RunContext run,
      Map<String, Object> initial,
      Map<String, Object> result,
      List<RequestResult> requests,
      List<RaceEvent> events) {
    long onCall = number(result, "on_call");
    boolean holds = onCall >= 1;
    return new InvariantResult(
        "at least one doctor stays on call",
        holds,
        "≥ 1 doctor on call",
        onCall + " doctor(s) on call",
        holds ? "The ward is covered." : "Nobody is on call: every request's check passed on its own snapshot.");
  }

  @Override
  public String conclusion(RunContext run, InvariantResult invariant, List<RequestResult> requests) {
    if (invariant.holds()) return Texts.preserved(run.config(), requests);
    return "Each doctor's transaction counted the others on call in its own snapshot and changed"
        + " only its own row. Because the writes touched different rows, no lock conflicted and no"
        + " version check fired: each transaction was consistent on its own, and together they"
        + " broke a rule that spans rows.";
  }

  @Override
  public Explanation.Fix fix(RunConfig config, InvariantResult invariant) {
    if (invariant.holds()) return null;
    return Texts.fix(
        Mode.SERIALIZABLE,
        Isolation.SERIALIZABLE,
        "The rule spans rows, so the fix must see reads as well as writes: SERIALIZABLE aborts one"
            + " of two transactions that each read what the other wrote.");
  }
}
