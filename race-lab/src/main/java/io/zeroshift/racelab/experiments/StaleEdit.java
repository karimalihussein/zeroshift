package io.zeroshift.racelab.experiments;

import static io.zeroshift.racelab.experiments.Texts.mode;
import static io.zeroshift.racelab.experiments.Texts.number;

import io.zeroshift.racelab.application.Participant;
import io.zeroshift.racelab.application.Participant.Read;
import io.zeroshift.racelab.application.Participant.Row;
import io.zeroshift.racelab.application.Participant.Write;
import io.zeroshift.racelab.application.port.LabDatabase;
import io.zeroshift.racelab.domain.ExperimentInfo;
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
 * 5. Two editors save the same document: version N → N+1, and the stale writer rejected. Each save
 * also records a revision naming the version it was based on, so an overwrite leaves evidence.
 */
public class StaleEdit extends ReadDecideWrite {
  static final String READ = "SELECT title, version FROM document WHERE id = ?";

  @Override
  public ExperimentInfo info() {
    return new ExperimentInfo(
        "optimistic-conflict",
        5,
        "Optimistic locking conflict",
        "Versions",
        "Two people open the same document, edit, and press Save. Whose edit survives, and does"
            + " anyone know the other's was lost?",
        "An editor loads version N, works for a while (no lock is held), then saves. The version"
            + " column is the guard: a save writes N+1 only WHERE version = N. The first save moves"
            + " the row to N+1; the second save still says N, matches no row, and is rejected"
            + " instead of silently replacing the first editor's work.",
        "no save replaces a version it did not read",
        "Initial version",
        List.of(
            mode(
                Mode.UNSAFE,
                "Save writes the new title and version N+1 without checking that the row is still at"
                    + " N. The second save overwrites the first; both editors see \"saved\".",
                "SELECT title, version FROM document WHERE id = ?;\n-- user edits…\n"
                    + "UPDATE document SET title = ?, version = :v + 1 WHERE id = ?;",
                Isolation.READ_COMMITTED,
                false),
            mode(
                Mode.OPTIMISTIC,
                "Save only if the version is unchanged: UPDATE … WHERE version = N. A stale save"
                    + " matches no row; the lab re-reads the row to show expected versus actual"
                    + " version, then rolls back. With retries, the editor re-applies the edit on"
                    + " top of the newer version.",
                "UPDATE document SET title = ?, version = :v + 1\n WHERE id = ? AND version = :v;\n"
                    + "-- 0 rows: someone saved first → reload, merge, retry",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.PESSIMISTIC,
                "Lock the document when opening it for edit. The second editor cannot even load it"
                    + " until the first has saved: safe, but it holds a lock for the whole edit.",
                "SELECT title, version FROM document WHERE id = ? FOR UPDATE;",
                Isolation.READ_COMMITTED,
                true),
            mode(
                Mode.SERIALIZABLE,
                "The unsafe save at SERIALIZABLE: the second save aborts with 40001.",
                "BEGIN ISOLATION LEVEL SERIALIZABLE;\n-- unsafe save\n-- 40001",
                Isolation.SERIALIZABLE,
                true)),
        Mode.UNSAFE,
        Isolation.READ_COMMITTED,
        new ExperimentInfo.Limits(2, 10, 2, 1, 1000, 4, 200, 0),
        List.of(),
        "A read v4 · B read v4 · A saved v5 · B saved v5 over it → A's edit is gone and nobody was"
            + " told.");
  }

  @Override
  public Map<String, Object> seed(LabDatabase db, RunContext run) {
    long doc =
        db.insert(
            "INSERT INTO document(run_id, title, version) VALUES (?, 'Q3 plan', ?) RETURNING id",
            run.runId(),
            run.config().initialValue());
    return Map.of("run", run.runId(), "document", doc);
  }

  @Override
  public Map<String, Object> observe(LabDatabase db, RunContext run) {
    return db.one(
        "SELECT d.title, d.version, (SELECT count(*) FROM document_revision r WHERE r.document_id = d.id)"
            + " AS saves, (SELECT count(DISTINCT r.based_on) FROM document_revision r WHERE r.document_id = d.id)"
            + " AS bases FROM document d WHERE d.id = ?",
        run.key("document"));
  }

  @Override
  public String describe(Map<String, Object> state) {
    return "v" + number(state, "version") + " · \"" + state.get("title") + "\"";
  }

  static String title(Participant p) {
    return "Q3 plan (edited by " + p.lane() + ")";
  }

  @Override
  String target(Participant p) {
    return "document #" + p.key("document");
  }

  @Override
  Read read(Participant p, boolean lock) {
    return Read.of(target(p), READ + (lock ? " FOR UPDATE" : ""), "version", p.key("document"))
        .versioned("version");
  }

  @Override
  Decision decide(Participant p, Row row) {
    return null; // editing is always allowed; the question is what the save does
  }

  @Override
  Write blindWrite(Participant p, Row row) {
    long next = row.number("version") + 1;
    return Write.update(
            target(p),
            "UPDATE document SET title = ?, version = ? WHERE id = ?",
            "version=" + next,
            title(p),
            next,
            p.key("document"))
        .version(next);
  }

  @Override
  Write conditionalWrite(Participant p, Row row) {
    return versionedWrite(p, row);
  }

  @Override
  Write versionedWrite(Participant p, Row row) {
    long v = row.number("version");
    return Write.update(
            target(p),
            "UPDATE document SET title = ?, version = ? WHERE id = ? AND version = ?",
            "version=" + (v + 1),
            title(p),
            v + 1,
            p.key("document"),
            v)
        .version(v + 1);
  }

  @Override
  String conditionFailed(Participant p) {
    return "stale save rejected";
  }

  @Override
  Write record(Participant p, Row row) {
    long v = row.number("version");
    return Write.insert(
        "revisions",
        "INSERT INTO document_revision(run_id, document_id, based_on, version, title, request_id)"
            + " VALUES (?, ?, ?, ?, ?, ?)",
        "revision v" + v + " → v" + (v + 1),
        p.key("run"),
        p.key("document"),
        v,
        v + 1,
        title(p),
        p.identity().requestId());
  }

  @Override
  String succeeded(Participant p, Row row) {
    long v = row.number("version");
    return "saved v" + (v + 1) + " on top of v" + v;
  }

  @Override
  public InvariantResult check(
      RunContext run,
      Map<String, Object> initial,
      Map<String, Object> result,
      List<RequestResult> requests,
      List<RaceEvent> events) {
    long saves = number(result, "saves");
    long bases = number(result, "bases");
    long version = number(result, "version");
    long start = run.config().initialValue();
    boolean holds = saves == bases && version == start + saves;
    return new InvariantResult(
        "no save replaces a version it did not read",
        holds,
        "every save based on a different version; final version = " + start + " + saves",
        saves + " save(s) based on " + bases + " distinct version(s), final version " + version,
        holds
            ? "Each save was based on the version it replaced."
            : (saves - bases)
                + " save(s) were based on a version another save had already replaced: an edit"
                + " was overwritten without anyone being told.");
  }

  @Override
  public String conclusion(RunContext run, InvariantResult invariant, List<RequestResult> requests) {
    if (invariant.holds()) return Texts.preserved(run.config(), requests);
    return "Both editors started from the same version and both saves wrote the next version"
        + " number. The document shows only the last save; the revision table shows two saves"
        + " based on the same version, the fingerprint of a lost edit.";
  }

  @Override
  public Explanation.Fix fix(RunConfig config, InvariantResult invariant) {
    if (invariant.holds()) return null;
    return Texts.fix(
        Mode.OPTIMISTIC,
        Isolation.READ_COMMITTED,
        "Save WHERE version = the version you loaded. A stale save matches no row and the editor"
            + " is told to reload instead of overwriting.");
  }
}
