package io.zeroshift.kafkalab;

import io.zeroshift.platform.web.ApiException;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import java.util.List;
import java.util.Locale;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

/** The Kafka lab's HTTP levers. Every one acts on the real lab cluster or its containers. */
@RestController
@RequestMapping("/api/kafka-lab")
public class KafkaLabController {
  public record Runs(
      List<KafkaLabRuns.Run> failover,
      List<KafkaLabRuns.Run> acks,
      List<KafkaLabRuns.Run> retries,
      List<KafkaLabRuns.Run> unclean,
      List<KafkaLabRuns.Run> delivery) {}

  public record State(ClusterObserver.Snapshot cluster, Traffic.View traffic, Runs runs) {}

  public record Elected(int partitionsMoved) {}

  public record LogLines(int node, String partition, List<String> lines) {}

  private final ClusterObserver observer;
  private final LabNodes nodes;
  private final LabSetup setup;
  private final Traffic traffic;
  private final DurabilityLab durability;
  private final DeliveryLab delivery;
  private final KafkaLabRuns runs;

  public KafkaLabController(
      ClusterObserver observer,
      LabNodes nodes,
      LabSetup setup,
      Traffic traffic,
      DurabilityLab durability,
      DeliveryLab delivery,
      KafkaLabRuns runs) {
    this.observer = observer;
    this.nodes = nodes;
    this.setup = setup;
    this.traffic = traffic;
    this.durability = durability;
    this.delivery = delivery;
    this.runs = runs;
  }

  @GetMapping("/state")
  public State state() {
    return new State(
        observer.snapshot(),
        traffic.view(),
        new Runs(
            runs.recent(KafkaLabRuns.FAILOVER, 5),
            runs.recent(KafkaLabRuns.ACKS, 5),
            runs.recent(KafkaLabRuns.RETRIES, 5),
            runs.recent(KafkaLabRuns.UNCLEAN, 5),
            runs.recent(KafkaLabRuns.DELIVERY, 8)));
  }

  /** Kill, stop, start, pause or unpause one node's container. */
  @PostMapping("/nodes/{id}/{action}")
  public ClusterObserver.Snapshot node(
      @PathVariable @Min(1) @Max(3) int id, @PathVariable String action) {
    nodes.act(id, LabNodes.Action.parse(action));
    return observer.observe();
  }

  /**
   * The node's own log lines about truncating {@code partition}: a returning replica throwing away
   * records the new leader does not have.
   */
  @GetMapping("/nodes/{id}/truncations")
  public LogLines truncations(
      @PathVariable @Min(1) @Max(3) int id,
      @RequestParam @Pattern(regexp = "lab\\.[a-z.]+-[0-9]+") String partition,
      @RequestParam(required = false) java.time.Instant since) {
    var from = since == null ? java.time.Instant.now().minusSeconds(1800) : since;
    return new LogLines(id, partition, nodes.logLines(id, from, partition, "Truncat"));
  }

  @PostMapping("/setup")
  public ClusterObserver.Snapshot setup() {
    setup.ensureStanding();
    return observer.observe();
  }

  /** Every node started and unfrozen; waits until all three are registered brokers again. */
  @PostMapping("/recover")
  public ClusterObserver.Snapshot recover() {
    return setup.recoverAll();
  }

  /** Every node up and unfrozen, scenario topics gone, standing topics present. */
  @PostMapping("/reset")
  public ClusterObserver.Snapshot reset() {
    return setup.reset();
  }

  @PostMapping("/elections/preferred")
  public Elected electPreferred() {
    return new Elected(setup.electPreferred());
  }

  @PostMapping("/traffic/start")
  public Traffic.View startTraffic() {
    return traffic.start();
  }

  @PostMapping("/traffic/stop")
  public Traffic.View stopTraffic() {
    return traffic.stop();
  }

  /** One write, with the chosen acks, reporting the offset or the broker's refusal. */
  @PostMapping("/probe")
  public DurabilityLab.Probe probe(
      @RequestParam(defaultValue = LabTopics.DURABILITY) String topic,
      @RequestParam(defaultValue = "all") @Pattern(regexp = "0|1|all") String acks) {
    return durability.probe(topic, acks);
  }

  @PutMapping("/topics/{topic}/min-insync-replicas")
  public ClusterObserver.TopicView minInsyncReplicas(
      @PathVariable String topic, @RequestParam @Min(1) @Max(3) int value) {
    return durability.minInsyncReplicas(topic, value);
  }

  @PostMapping("/scenarios/acks")
  public KafkaLabRuns.Run acks(@RequestParam @Pattern(regexp = "1|all") String acks) {
    return durability.acks(acks);
  }

  @PostMapping("/scenarios/retries")
  public KafkaLabRuns.Run retries(@RequestParam boolean idempotent) {
    return durability.retries(idempotent);
  }

  @PostMapping("/scenarios/unclean/{phase}")
  public KafkaLabRuns.Run unclean(@PathVariable String phase) {
    return switch (phase) {
      case "break" -> durability.uncleanBreak();
      case "elect" -> durability.uncleanElect();
      case "check" -> durability.uncleanCheck();
      default ->
          throw new ApiException(
              HttpStatus.NOT_FOUND,
              "UNKNOWN_PHASE",
              "The unclean scenario has break, elect and check, not " + phase);
    };
  }

  @PostMapping("/delivery")
  public KafkaLabRuns.Run delivery(
      @RequestParam String mode, @RequestParam(defaultValue = "false") boolean crash) {
    DeliveryWorker.Mode parsed;
    try {
      parsed = DeliveryWorker.Mode.valueOf(mode.toUpperCase(Locale.ROOT).replace('-', '_'));
    } catch (IllegalArgumentException e) {
      throw new ApiException(
          HttpStatus.BAD_REQUEST,
          "UNKNOWN_DELIVERY_MODE",
          "Mode is at-most-once, at-least-once or exactly-once, not " + mode);
    }
    return delivery.run(parsed, crash);
  }
}
