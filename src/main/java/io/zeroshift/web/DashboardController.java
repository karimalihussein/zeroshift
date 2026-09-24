package io.zeroshift.web;

import io.zeroshift.application.*;
import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import java.util.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
public class DashboardController {
  public record Counts(long customers, long orders) {
    public long total() {
      return customers + orders;
    }
  }

  public record Completion(
      boolean successful,
      boolean validationPassed,
      Long cdcPending,
      long totalMigratedRows,
      Long durationMillis) {}

  /**
   * Reverse sync after cutover. {@code dataLost} is reported only for a completed rollback, where
   * final validation matched both databases behind both fences with nothing left to replay.
   */
  public record Rollback(
      boolean available,
      boolean inProgress,
      boolean abortable,
      boolean completed,
      double progress,
      long pending,
      long applied,
      long conflicts,
      String validation,
      boolean validationPassed,
      Long durationMillis,
      Long freezeMillis,
      long verifiedRows,
      Long dataLost) {}

  public record Dashboard(
      MigrationState migration,
      double progress,
      Counts source,
      Counts target,
      Long pending,
      List<String> logs,
      TrafficMetrics traffic,
      String captureError,
      int seedRows,
      boolean migrationStartAllowed,
      Completion completion,
      Rollback rollback,
      List<LogEvent> events) {}

  public record ActionResult(String message) {}

  private final MigrationStore store;
  private final SourceDatabase source;
  private final MigrationCoordinator migration;
  private final TrafficSimulator traffic;
  private final DemoDataService demo;
  private final CutoverService cutover;
  private final ChangeCatchUp catchUp;
  private final RollbackService rollback;

  public DashboardController(
      MigrationStore store,
      SourceDatabase source,
      MigrationCoordinator migration,
      TrafficSimulator traffic,
      DemoDataService demo,
      CutoverService cutover,
      ChangeCatchUp catchUp,
      RollbackService rollback) {
    this.store = store;
    this.source = source;
    this.migration = migration;
    this.traffic = traffic;
    this.demo = demo;
    this.cutover = cutover;
    this.catchUp = catchUp;
    this.rollback = rollback;
  }

  @GetMapping("/")
  public String index() {
    return "index";
  }

  @GetMapping("/api/status")
  @ResponseBody
  public Dashboard status() {
    var state = store.state();
    var sourceCounts = new Counts(source.count(Table.CUSTOMERS), source.count(Table.ORDERS));
    var targetCounts = new Counts(store.count(Table.CUSTOMERS), store.count(Table.ORDERS));
    Long pending = null;
    String error = "";
    if (state.stage() == Stage.COMPLETED) {
      pending = 0L;
    } else if (state.stage() != Stage.IDLE) {
      try {
        pending = catchUp.pending(store);
      } catch (MigrationException e) {
        error = e.getMessage();
      }
    }
    return new Dashboard(
        state,
        state.progress(),
        sourceCounts,
        targetCounts,
        pending,
        store.logs(),
        traffic.metrics(),
        error,
        demo.defaultRows(),
        state.stage() == Stage.IDLE && sourceCounts.total() > 0,
        new Completion(
            state.successful(),
            state.validationPassed(),
            pending,
            state.successful() ? state.completedRows() : targetCounts.total(),
            state.durationMillis()),
        rollback(state),
        store.logEvents());
  }

  private Rollback rollback(MigrationState state) {
    var reverse = store.rollback();
    boolean tracked = reverse.captureActive() && state.primary() == Primary.POSTGRESQL;
    boolean completed = state.rolledBack();
    return new Rollback(
        state.successful() && reverse.captureActive(),
        state.stage().rollback() && !completed,
        state.stage().rollbackAbortable(),
        completed,
        state.rollbackProgress(),
        tracked ? store.reversePending() : 0,
        reverse.applied(),
        reverse.conflicts(),
        reverse.validation(),
        reverse.validationPassed(),
        reverse.durationMillis(),
        reverse.freezeMillis(),
        reverse.verifiedRows(),
        completed && reverse.validationPassed() ? 0L : null);
  }

  @PostMapping("/api/actions/{action}")
  @ResponseBody
  public ActionResult action(
      @PathVariable String action, @RequestParam(required = false) Integer rows) {
    switch (action) {
      case "seed" -> demo.seed(rows);
      case "traffic-start" -> traffic.toggle(true);
      case "traffic-stop" -> traffic.toggle(false);
      case "start" -> migration.start();
      case "pause" -> migration.pause();
      case "resume" -> migration.resume();
      case "crash" -> migration.crash();
      case "validate" -> {
        var result = cutover.inspect();
        return new ActionResult(result.matches() ? "Validation passed" : "Validation FAILED");
      }
      case "cutover" -> cutover.request();
      case "rollback" -> rollback.request();
      case "rollback-abort" -> rollback.abort();
      case "reset" -> demo.reset();
      default -> throw new InvalidAction("Unknown action");
    }
    return new ActionResult("Action accepted: " + action);
  }
}
