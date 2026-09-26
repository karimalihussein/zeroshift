package io.zeroshift.resilience;

import io.zeroshift.eventlab.EventLabSettings;
import io.zeroshift.eventlab.LabServices;
import io.zeroshift.kafkalab.KafkaLabRuns;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.JsonNode;

/** The resilience lab: its page, its readings and every lever it offers. */
@Controller
@Validated
public class ResilienceController {
  public record Chaos(boolean configured, List<Toxiproxy.Link> links, String error) {}

  public record State(
      long now,
      Chaos chaos,
      LoadGenerator.Totals load,
      ExperimentRunner.Run run,
      List<Sample> samples,
      List<ExperimentRunner.Marker> markers) {}

  public record Controls(
      Map<String, JsonNode> edge, JsonNode gateway, JsonNode payment, List<String> unreachable) {}

  public record ExperimentView(
      String id,
      String title,
      String summary,
      List<String> concepts,
      boolean needsChaos,
      LoadGenerator.Profile load,
      List<StepView> steps) {}

  public record StepView(Experiment.Phase phase, String title, String text, List<String> checks) {}

  private final EventLabSettings settings;
  private final LabServices services;
  private final Sampler sampler;
  private final LoadGenerator load;
  private final Levers levers;
  private final ExperimentCatalog catalog;
  private final ExperimentRunner runner;
  private final KafkaLabRuns runs;

  public ResilienceController(
      EventLabSettings settings,
      LabServices services,
      Sampler sampler,
      LoadGenerator load,
      Levers levers,
      ExperimentCatalog catalog,
      ExperimentRunner runner,
      KafkaLabRuns runs) {
    this.settings = settings;
    this.services = services;
    this.sampler = sampler;
    this.load = load;
    this.levers = levers;
    this.catalog = catalog;
    this.runner = runner;
    this.runs = runs;
  }

  @GetMapping("/resilience")
  public String page(Model model) {
    model.addAttribute("grafanaUrl", settings.grafanaUrl());
    return "resilience";
  }

  /**
   * Samples and markers newer than {@code since} (epoch ms; 0 for the last three minutes), the load
   * generator, the chaos links and the running experiment. Polling this keeps the sampler on.
   */
  @GetMapping("/api/resilience/state")
  @ResponseBody
  public State state(@RequestParam(defaultValue = "0") long since) {
    sampler.watched();
    var samples = since == 0 ? sampler.last(180) : sampler.since(since);
    return new State(
        System.currentTimeMillis(),
        chaos(),
        load.totals(),
        runner.current(),
        samples,
        runner.markers(since == 0 ? System.currentTimeMillis() - 900_000 : since));
  }

  /** The levers' current settings, read from the services. */
  @GetMapping("/api/resilience/controls")
  @ResponseBody
  public Controls controls() {
    var edge = new LinkedHashMap<String, JsonNode>();
    var unreachable = new java.util.ArrayList<String>();
    for (var r : Levers.ORDER_REPLICAS) {
      var answer = services.tryGet(r, "/lab/edge");
      if (answer.has("error")) unreachable.add(r);
      edge.put(r, answer);
    }
    var gateway = services.tryGet(Levers.PAYMENT, "/lab/gateway");
    var payment = services.tryGet(Levers.PAYMENT, "/lab/state");
    if (payment.has("error")) unreachable.add(Levers.PAYMENT);
    return new Controls(edge, gateway, payment, unreachable);
  }

  @GetMapping("/api/resilience/experiments")
  @ResponseBody
  public List<ExperimentView> experiments() {
    return catalog.all().stream()
        .map(
            e ->
                new ExperimentView(
                    e.id(),
                    e.title(),
                    e.summary(),
                    e.concepts(),
                    e.needsChaos(),
                    e.load(),
                    e.steps().stream()
                        .map(
                            s ->
                                new StepView(
                                    s.phase(),
                                    s.title(),
                                    s.text(),
                                    s.checks().stream().map(Experiment.Check::claim).toList()))
                        .toList()))
        .toList();
  }

  @GetMapping("/api/resilience/runs")
  @ResponseBody
  public List<KafkaLabRuns.Run> runs(
      @RequestParam(defaultValue = "10") @Min(1) @Max(50) int limit) {
    return runs.recent(ExperimentRunner.LAB, limit);
  }

  // ---- Load ----------------------------------------------------------------------------------

  @PostMapping("/api/resilience/load/start")
  @ResponseBody
  public LoadGenerator.Totals startLoad(@RequestBody LoadGenerator.Profile profile) {
    var totals = load.start(profile);
    runner.mark("load", "Load " + profile.ratePerSecond() + "/s started");
    return totals;
  }

  @PutMapping("/api/resilience/load")
  @ResponseBody
  public LoadGenerator.Totals updateLoad(@RequestBody LoadGenerator.Profile profile) {
    var totals = load.update(profile);
    runner.mark("load", "Load " + profile.ratePerSecond() + "/s, retry " + profile.retry());
    return totals;
  }

  @PostMapping("/api/resilience/load/stop")
  @ResponseBody
  public LoadGenerator.Totals stopLoad() {
    runner.mark("load", "Load stopped");
    return load.stop();
  }

  @PostMapping("/api/resilience/load/reset")
  @ResponseBody
  public LoadGenerator.Totals resetLoad() {
    return load.reset();
  }

  // ---- Chaos ---------------------------------------------------------------------------------

  @PostMapping("/api/resilience/chaos/{link}/{fault}")
  @ResponseBody
  public Chaos inject(
      @PathVariable String link,
      @PathVariable String fault,
      @RequestParam(defaultValue = "0") @Min(0) @Max(60_000) int value,
      @RequestParam(defaultValue = "0") @Min(0) @Max(10_000) int jitter) {
    if (fault.equals("heal")) {
      levers.chaos().heal(link);
      runner.mark("recover", link + " healed");
    } else {
      var parsed = Toxiproxy.Fault.parse(fault);
      levers.chaos().inject(link, parsed, value, jitter);
      runner.mark("inject", link + ": " + fault + (value > 0 ? " " + value : ""));
    }
    return chaos();
  }

  @PostMapping("/api/resilience/chaos/heal")
  @ResponseBody
  public Chaos healAll() {
    levers.chaos().healAll();
    runner.mark("recover", "All links healed");
    return chaos();
  }

  // ---- Guards, policy, consumers ---------------------------------------------------------------

  @PutMapping("/api/resilience/edge")
  @ResponseBody
  public Map<String, JsonNode> edge(@RequestBody Levers.Edge edge) {
    var result = levers.edge(edge);
    runner.mark("mitigate", "Edge: " + describe(edge));
    return result;
  }

  @PutMapping("/api/resilience/policy")
  @ResponseBody
  public JsonNode policy(@RequestBody Levers.Policy policy) {
    var result = levers.policy(policy);
    runner.mark(
        "mitigate",
        "Gateway: timeout "
            + policy.timeoutMs()
            + " ms, retry "
            + policy.retry()
            + ", breaker "
            + (policy.breaker() ? "on" : "off")
            + (policy.pauseOnOpen() ? ", pause on open" : ""));
    return result;
  }

  @PostMapping("/api/resilience/gateway/{mode}")
  @ResponseBody
  public JsonNode gateway(
      @PathVariable @Pattern(regexp = "healthy|slow|down|declining") String mode) {
    var result = levers.gateway(mode);
    runner.mark(mode.equals("healthy") ? "recover" : "inject", "Gateway " + mode);
    return result;
  }

  @PostMapping("/api/resilience/breaker/reset")
  @ResponseBody
  public JsonNode resetBreaker() {
    return levers.resetBreaker();
  }

  /** A slow consumer on {@code service}; 0 ms makes it normal again. */
  @PostMapping("/api/resilience/slow")
  @ResponseBody
  public JsonNode slow(
      @RequestParam
          @Pattern(regexp = "order-service|payment-service|inventory-service|shipping-service")
          String service,
      @RequestParam @Min(0) @Max(30_000) int ms) {
    if (ms == 0) {
      runner.mark("recover", service + " consumer speed normal");
      return levers.clearFault(service, "slow-processing");
    }
    runner.mark("inject", service + " consumer " + ms + " ms/record");
    return levers.slowConsumer(service, ms);
  }

  @PostMapping("/api/resilience/consumers/payment/{action}")
  @ResponseBody
  public JsonNode paymentConsumer(@PathVariable @Pattern(regexp = "pause|resume") String action) {
    runner.mark(
        action.equals("pause") ? "mitigate" : "recover", "Payment consumer " + action + "d");
    return levers.consumer(Levers.PAYMENT, Levers.PAYMENT, action);
  }

  /** Without parameters, the consumer factory's defaults again. */
  @PutMapping("/api/resilience/consumers/payment/config")
  @ResponseBody
  public JsonNode paymentConsumerConfig(
      @RequestParam(required = false) @Min(1) @Max(10_000) Integer maxPollRecords,
      @RequestParam(required = false) @Min(1_000) @Max(600_000) Integer maxPollIntervalMs) {
    runner.mark(
        "note",
        maxPollRecords == null && maxPollIntervalMs == null
            ? "Payment consumer: default poll settings"
            : "Payment consumer: max.poll.records="
                + maxPollRecords
                + ", max.poll.interval.ms="
                + maxPollIntervalMs);
    return levers.consumerConfig(Levers.PAYMENT, Levers.PAYMENT, maxPollRecords, maxPollIntervalMs);
  }

  // ---- Experiments ---------------------------------------------------------------------------

  @PostMapping("/api/resilience/experiments/{id}/start")
  @ResponseBody
  public ExperimentRunner.Run start(
      @PathVariable String id, @RequestParam(defaultValue = "false") boolean auto) {
    return runner.start(id, auto);
  }

  @PostMapping("/api/resilience/experiments/next")
  @ResponseBody
  public ExperimentRunner.Run next(@RequestParam(defaultValue = "false") boolean auto) {
    return runner.next(auto);
  }

  @PostMapping("/api/resilience/experiments/abort")
  @ResponseBody
  public ExperimentRunner.Run abort() {
    return runner.abort();
  }

  /** Everything back to defaults and the load stopped. Failures of single levers are listed. */
  @PostMapping("/api/resilience/reset")
  @ResponseBody
  public Map<String, Object> reset() {
    load.stop();
    var failures = levers.resetAll();
    runner.mark("reset", "Lab reset");
    return Map.of("failures", failures);
  }

  private Chaos chaos() {
    if (!levers.chaos().configured()) return new Chaos(false, List.of(), null);
    try {
      return new Chaos(true, levers.chaos().links(), null);
    } catch (RuntimeException e) {
      return new Chaos(true, List.of(), e.getClass().getSimpleName() + ": " + e.getMessage());
    }
  }

  private static String describe(Levers.Edge e) {
    var parts = new java.util.ArrayList<String>();
    if (e.rateLimit()) parts.add("rate limit " + e.ratePerSecond() + "/s");
    if (e.shedding()) parts.add("shed above " + e.maxActiveSagas());
    if (e.bulkhead()) parts.add("bulkhead " + e.maxConcurrent());
    return parts.isEmpty() ? "guards off" : String.join(", ", parts);
  }
}
