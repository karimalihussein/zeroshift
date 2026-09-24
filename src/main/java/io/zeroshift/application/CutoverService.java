package io.zeroshift.application;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;

public final class CutoverService {
  private final SourceDatabase source;
  private final MigrationStore store;
  private final ChangeCatchUp catchUp;
  private final ValidationService validation;

  public CutoverService(
      SourceDatabase source,
      MigrationStore store,
      ChangeCatchUp catchUp,
      ValidationService validation) {
    this.source = source;
    this.store = store;
    this.catchUp = catchUp;
    this.validation = validation;
  }

  public void request() {
    store.transaction(
        s -> {
          var state = s.state();
          state.require(
              state.stage() == Stage.READY && state.active() && !state.cdcPaused(),
              "Wait for Ready and resume migration and CDC replay before cutover");
          s.stage(Stage.FREEZE);
          s.log("Cutover requested; routed traffic waits while source writes are fenced");
          return null;
        });
  }

  public void finish(MigrationStore.Session session) {
    // FREEZE was persisted before touching SQL Server. A crash here is safely resumable.
    long started = System.nanoTime();
    source.freeze(true);
    catchUp.drain(session);
    var result = validation.validate();
    session.validation(result);
    if (!result.matches())
      throw new MigrationException(
          "Cutover blocked: validation failed. Source remains fenced; repair or Reset.");
    session.synchronizeSequences();
    session.complete();
    session.log(
        "Final fence, catch-up, validation and sequence sync: "
            + ((System.nanoTime() - started) / 1_000_000)
            + " ms");
  }

  public ValidationResult inspect() {
    return store.transaction(
        s -> {
          s.state()
              .require(
                  s.state().stage() == Stage.READY && !s.state().cdcPaused(),
                  "Validation requires Ready with CDC replay resumed");
          source.freeze(true);
          try {
            catchUp.drain(s);
            var result = validation.validate();
            s.validation(result);
            return result;
          } finally {
            source.freeze(false);
          }
        });
  }
}
