package io.zeroshift.racelab.web;

import io.zeroshift.platform.web.ApiResponse;
import io.zeroshift.racelab.application.ExperimentEngine;
import io.zeroshift.racelab.application.RunComparison;
import io.zeroshift.racelab.application.port.RunRepository;
import io.zeroshift.racelab.domain.ExperimentInfo;
import io.zeroshift.racelab.domain.RaceEvent;
import io.zeroshift.racelab.domain.RaceLabErrors;
import io.zeroshift.racelab.domain.Run;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * The race lab's HTTP API. It binds, validates and delegates: the engine creates and runs
 * experiments, the repository answers reads, the stream serves live timelines.
 */
@RestController
@RequestMapping("/api/race-lab")
public class RaceLabController {
  private final ExperimentEngine engine;
  private final RunRepository runs;
  private final RunComparison comparison;
  private final RunEventStream stream;
  private final String grafanaUrl;

  public RaceLabController(
      ExperimentEngine engine,
      RunRepository runs,
      RunComparison comparison,
      RunEventStream stream,
      RaceLabSettings settings) {
    this.engine = engine;
    this.runs = runs;
    this.comparison = comparison;
    this.stream = stream;
    this.grafanaUrl = settings.grafanaUrl();
  }

  @GetMapping("/experiments")
  public ApiResponse<List<ExperimentInfo>> experiments() {
    return ApiResponse.list(engine.experiments().catalog());
  }

  @GetMapping("/state")
  public RaceLabDtos.LabState state() {
    long running = engine.running();
    return new RaceLabDtos.LabState(running > 0 ? running : null, grafanaUrl);
  }

  /** Creates a run with fresh request, order and customer ids; it starts with {@code /start}. */
  @PostMapping("/runs")
  @ResponseStatus(HttpStatus.CREATED)
  public Run create(@Valid @RequestBody RaceLabDtos.CreateRun request) {
    return engine.create(request.toConfig());
  }

  /** Starts a created run; follow it on {@code /stream} or poll {@code /runs/{id}}. */
  @PostMapping("/runs/{id}/start")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public Run start(@PathVariable long id) {
    return engine.start(id);
  }

  @GetMapping("/runs")
  public ApiResponse<List<RaceLabDtos.RunSummary>> runs(
      @RequestParam(required = false) @Pattern(regexp = "[a-z-]{1,40}") String experiment,
      @RequestParam(defaultValue = "20") @Min(1) @Max(100) int limit) {
    var fetched =
        runs.recent(experiment, limit + 1).stream().map(RaceLabDtos.RunSummary::of).toList();
    return ApiResponse.page(fetched, limit);
  }

  @GetMapping("/runs/{id}")
  public Run run(@PathVariable long id) {
    return runs.find(id).orElseThrow(() -> new RaceLabErrors.RunNotFound(id));
  }

  /** The run's events in order, after {@code after}. */
  @GetMapping("/runs/{id}/events")
  public ApiResponse<List<RaceEvent>> events(
      @PathVariable long id,
      @RequestParam(defaultValue = "0") @Min(0) int after,
      @RequestParam(defaultValue = "2000") @Min(1) @Max(5000) int limit) {
    runs.find(id).orElseThrow(() -> new RaceLabErrors.RunNotFound(id));
    return ApiResponse.page(runs.events(id, after, limit + 1), limit);
  }

  /** Server-sent events: {@code event} for each recorded event, {@code run} for the run's state. */
  @GetMapping(path = "/runs/{id}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public SseEmitter stream(
      @PathVariable long id, @RequestParam(defaultValue = "0") @Min(0) int after) {
    return stream.open(id, after);
  }

  @GetMapping("/compare")
  public RunComparison.Comparison compare(@RequestParam long left, @RequestParam long right) {
    return comparison.compare(left, right);
  }

  /** Deletes the lab's runs and scenario rows (race_lab only); ids restart at 1. */
  @PostMapping("/reset")
  public RaceLabDtos.Reset reset() {
    return new RaceLabDtos.Reset(engine.reset());
  }
}
