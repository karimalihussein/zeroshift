package io.zeroshift.application;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Rollback after cutover without losing PostgreSQL's post-cutover writes. Every stage is persisted
 * before its work runs, so a crash or restart resumes at the stage it reached.
 *
 * <p>A blocked rollback records its evidence (conflicts, validation) and marks itself FAILED in the
 * same transaction instead of throwing, so the evidence survives. Until SWITCH_PRIMARY, PostgreSQL
 * stays primary and the rollback can be abandoned.
 */
public final class RollbackService {
  private final MigrationStore store;
  private final SourceDatabase source;
  private final SourceWriteback writeback;
  private final ReverseCatchUp reverse;
  private final ValidationService validation;

  public RollbackService(
      MigrationStore store,
      SourceDatabase source,
      SourceWriteback writeback,
      ReverseCatchUp reverse,
      ValidationService validation) {
    this.store = store;
    this.source = source;
    this.writeback = writeback;
    this.reverse = reverse;
    this.validation = validation;
  }

  public void request() {
    store.transaction(
        s -> {
          s.state()
              .require(
                  s.state().successful(),
                  "Rollback is available only after a successful cutover to PostgreSQL");
          s.state()
              .require(
                  store.rollback().captureActive(),
                  "Reverse capture was not active since this cutover, so post-cutover writes cannot"
                      + " be proven complete. Reset and migrate again to enable rollback.");
          s.beginRollback();
          return null;
        });
  }

  public void abort() {
    store.transaction(
        s -> {
          s.state()
              .require(
                  s.state().stage().rollbackAbortable(),
                  "No rollback in progress that can still be abandoned");
          s.abortRollback();
          return null;
        });
  }

  public void advance(MigrationStore.Session session) {
    switch (session.state().stage()) {
      case ROLLBACK_PREPARE -> {
        if (blocked(session, reverse.conflicts(store.rollback().baselineVersion()))) return;
        session.log(
            "Reverse capture active since cutover; "
                + store.reversePending()
                + " changed keys to replay into SQL Server");
        session.stage(Stage.REVERSE_CATCH_UP);
      }
      case REVERSE_CATCH_UP -> {
        if (blocked(session, reverse.drain(session))) return;
        session.stage(Stage.ROLLBACK_VALIDATION);
      }
      case ROLLBACK_VALIDATION -> {
        if (blocked(session, reverse.drain(session))) return;
        ValidationResult result;
        try (var capture = store.reverseCapture()) {
          result = validation.validateSettled(capture);
        }
        session.rollbackValidation(result, "settled rows");
        if (!result.matches()) {
          fail(
              session,
              "Rollback blocked: SQL Server differs from PostgreSQL outside the pending changes."
                  + " Nothing was switched; PostgreSQL remains primary. Abort the rollback.");
          return;
        }
        session.stage(Stage.ROLLBACK_FREEZE);
      }
      case ROLLBACK_FREEZE -> {
        session.freezeWrites(true);
        session.stage(Stage.FINAL_SYNC);
      }
      case FINAL_SYNC -> {
        if (blocked(session, reverse.conflicts(store.rollback().baselineVersion()))) return;
        if (blocked(session, reverse.drain(session))) return;
        long pending = store.reversePending();
        if (pending != 0) {
          fail(
              session,
              "Rollback blocked: " + pending + " captured changes remain after final sync");
          return;
        }
        var result = validation.validate();
        session.rollbackValidation(result, "final");
        if (!result.matches()) {
          fail(
              session,
              "Rollback blocked: final validation failed. PostgreSQL remains primary and fenced;"
                  + " Resume to retry or Abort the rollback.");
          return;
        }
        session.stage(Stage.SWITCH_PRIMARY);
      }
      case SWITCH_PRIMARY -> {
        var issued = new java.util.EnumMap<Table, Long>(Table.class);
        for (var table : Table.values()) issued.put(table, store.issuedKey(table));
        writeback.synchronizeIdentities(issued);
        // Routing still reads PostgreSQL as primary until this transaction commits, so opening
        // SQL Server first cannot route a write to it early. A crash here repeats this stage.
        source.freeze(false);
        session.completeRollback();
      }
      default -> throw new InvalidAction("No rollback is in progress");
    }
  }

  private boolean blocked(MigrationStore.Session session, List<ReverseConflict> conflicts) {
    if (conflicts.isEmpty()) return false;
    session.conflicts(conflicts);
    fail(
        session,
        "Rollback blocked: "
            + conflicts.size()
            + " SQL Server rows changed outside ZeroShift after cutover ("
            + conflicts.stream().limit(5).map(Object::toString).collect(Collectors.joining(", "))
            + (conflicts.size() > 5 ? ", …" : "")
            + "). They were not overwritten. PostgreSQL remains primary; Abort the rollback and"
            + " reconcile SQL Server manually.");
    return true;
  }

  private static void fail(MigrationStore.Session session, String message) {
    session.status(RunStatus.FAILED, message);
  }
}
