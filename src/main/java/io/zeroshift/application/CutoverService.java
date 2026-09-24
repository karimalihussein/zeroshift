package io.zeroshift.application;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import java.time.Duration;

public final class CutoverService {
  private final SourceDatabase source;
  private final MigrationStore store;
  private final ChangeCatchUp catchUp;
  private final ValidationService validation;
  private final MigrationMetrics metrics;

  public CutoverService(
      SourceDatabase source,
      MigrationStore store,
      ChangeCatchUp catchUp,
      ValidationService validation,
      MigrationMetrics metrics) {
    this.source = source;
    this.store = store;
    this.catchUp = catchUp;
    this.validation = validation;
    this.metrics = metrics;
  }

  public void request() {
    store.transaction(
        s -> {
          var state = s.state();
          state.require(
              state.stage() == Stage.READY && state.active() && !state.cdcPaused(),
              "Wait for Ready and resume migration and CDC replay before cutover");
          s.beginCutover();
          return null;
        });
  }

  public void advance(MigrationStore.Session session) {
    switch (session.state().stage()) {
      case FREEZE -> {
        // FREEZE was persisted before touching SQL Server. A crash here is safely resumable.
        source.freeze(true);
        catchUp.drain(session);
        session.stage(Stage.VALIDATION);
      }
      case VALIDATION -> {
        var result = validation.validate();
        session.validation(result);
        if (!result.matches())
          throw new MigrationException(
              "Cutover blocked: validation failed. Source remains fenced; repair or Reset.");
        session.stage(Stage.CUTOVER);
      }
      case CUTOVER -> {
        session.synchronizeSequences();
        session.complete();
        var state = session.state();
        var elapsed = Duration.between(state.cutoverStartedAt(), state.completedAt());
        metrics.cutoverFinished(elapsed);
        session.log("Write freeze through successful cutover: " + elapsed.toMillis() + " ms");
      }
      default -> throw new InvalidAction("No cutover is in progress");
    }
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
