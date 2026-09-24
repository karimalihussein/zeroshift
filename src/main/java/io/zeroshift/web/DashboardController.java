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
      Completion completion) {}

  public record ActionResult(String message) {}

  private final MigrationStore store;
  private final SourceDatabase source;
  private final MigrationCoordinator migration;
  private final TrafficSimulator traffic;
  private final DemoDataService demo;
  private final CutoverService cutover;
  private final ChangeCatchUp catchUp;

  public DashboardController(
      MigrationStore store,
      SourceDatabase source,
      MigrationCoordinator migration,
      TrafficSimulator traffic,
      DemoDataService demo,
      CutoverService cutover,
      ChangeCatchUp catchUp) {
    this.store = store;
    this.source = source;
    this.migration = migration;
    this.traffic = traffic;
    this.demo = demo;
    this.cutover = cutover;
    this.catchUp = catchUp;
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
            state.durationMillis()));
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
      case "reset" -> demo.reset();
      default -> throw new InvalidAction("Unknown action");
    }
    return new ActionResult("Action accepted: " + action);
  }
}
