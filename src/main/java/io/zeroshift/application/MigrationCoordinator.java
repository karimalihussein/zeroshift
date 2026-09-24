package io.zeroshift.application;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;

public final class MigrationCoordinator {
  private final MigrationStore store;
  private final SourceDatabase source;
  private final SnapshotBatch snapshot;
  private final ChangeCatchUp catchUp;
  private final CutoverService cutover;
  private final RollbackService rollback;

  public MigrationCoordinator(
      MigrationStore store,
      SourceDatabase source,
      SnapshotBatch snapshot,
      ChangeCatchUp catchUp,
      CutoverService cutover,
      RollbackService rollback) {
    this.store = store;
    this.source = source;
    this.snapshot = snapshot;
    this.catchUp = catchUp;
    this.cutover = cutover;
    this.rollback = rollback;
  }

  public void recover() {
    store.transaction(
        s -> {
          if (s.state().active()) {
            s.status(RunStatus.PAUSED, "Backend restarted. Resume from the durable checkpoint.");
          }
          // An interrupted standalone Validate can leave a committed source fence.
          if (s.state().primary() == Primary.SQL_SERVER
              && !s.state().stage().sourceMustRemainFrozen()) source.freeze(false);
          return null;
        });
  }

  public void start() {
    store.transaction(
        s -> {
          s.state()
              .require(s.state().stage() == Stage.IDLE, "Reset before starting a new migration");
          var boundary = source.boundary();
          s.state()
              .require(
                  boundary.count() > 0,
                  "Cannot start migration: SQL Server has no customers or orders to migrate");
          s.start(boundary);
          return null;
        });
  }

  public void pause() {
    store.transaction(
        s -> {
          s.state()
              .require(
                  s.state().active() && s.state().stage().canPause(),
                  "No running migration to pause");
          s.status(RunStatus.PAUSED, "");
          return null;
        });
  }

  public void resume() {
    store.transaction(
        s -> {
          var state = s.state();
          state.require(
              state.stage().canResume() && !state.active(),
              "No paused or failed migration to resume");
          s.status(RunStatus.RUNNING, "");
          return null;
        });
  }

  public void crash() {
    store.transaction(
        s -> {
          s.state().require(s.state().active(), "Start or resume the migration first");
          s.requestCrash();
          return null;
        });
  }

  public void tick() {
    try {
      store.transaction(
          s -> {
            if (!s.state().active()) return null;
            switch (s.state().stage()) {
              case SNAPSHOT -> {
                snapshot.copy(s);
                if (!s.state().cdcPaused() && s.state().stage() == Stage.SNAPSHOT)
                  catchUp.drainCopied(s);
              }
              case CATCH_UP -> {
                if (s.state().cdcPaused()) break;
                catchUp.drain(s);
                s.stage(Stage.PREPARE);
              }
              case PREPARE -> {
                s.prepare();
                s.checkCrash();
                s.stage(Stage.READY);
              }
              case READY -> {
                if (!s.state().cdcPaused()) catchUp.drain(s);
              }
              case FREEZE, VALIDATION, CUTOVER -> cutover.advance(s);
              case ROLLBACK_PREPARE,
                  REVERSE_CATCH_UP,
                  ROLLBACK_VALIDATION,
                  ROLLBACK_FREEZE,
                  FINAL_SYNC,
                  SWITCH_PRIMARY ->
                  rollback.advance(s);
              case IDLE, COMPLETED, ROLLED_BACK -> {}
            }
            return null;
          });
    } catch (RuntimeException e) {
      store.transaction(
          s -> {
            if (s.state().active())
              s.status(
                  e instanceof SimulatedCrash ? RunStatus.CRASHED : RunStatus.FAILED,
                  safeMessage(e));
            return null;
          });
    }
  }

  public static String safeMessage(Exception error) {
    String message = error.getMessage();
    return message == null
        ? error.getClass().getSimpleName()
        : message.substring(0, Math.min(1500, message.length()));
  }
}
