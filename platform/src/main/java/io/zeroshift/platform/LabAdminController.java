package io.zeroshift.platform;

import java.util.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/**
 * The control plane's view into, and levers on, one service. Everything returned is read from the
 * live consumer containers or this service's database.
 */
@RestController
@RequestMapping("/lab")
public class LabAdminController {
  public record Consumer(
      String id,
      String groupId,
      List<String> topics,
      boolean running,
      boolean pauseRequested,
      boolean paused,
      List<String> assignedPartitions) {}

  public record State(String service, List<Consumer> consumers, List<Map<String, Object>> faults) {}

  private final String service;
  private final KafkaListenerEndpointRegistry registry;
  private final Faults faults;
  private final JdbcTemplate jdbc;

  public LabAdminController(
      @Value("${spring.application.name}") String service,
      KafkaListenerEndpointRegistry registry,
      Faults faults,
      JdbcTemplate jdbc) {
    this.service = service;
    this.registry = registry;
    this.faults = faults;
    this.jdbc = jdbc;
  }

  @GetMapping("/state")
  public State state() {
    var consumers =
        registry.getListenerContainers().stream()
            .map(
                c ->
                    new Consumer(
                        c.getListenerId(),
                        c.getGroupId(),
                        Arrays.asList(
                            Objects.requireNonNullElse(
                                c.getContainerProperties().getTopics(), new String[0])),
                        c.isRunning(),
                        c.isPauseRequested(),
                        c.isContainerPaused(),
                        Objects.requireNonNullElse(
                                c.getAssignedPartitions(), List.<TopicPartition>of())
                            .stream()
                            .map(TopicPartition::toString)
                            .sorted()
                            .toList()))
            .sorted(Comparator.comparing(Consumer::id))
            .toList();
    return new State(service, consumers, faults.armed());
  }

  @GetMapping("/decisions")
  public List<Map<String, Object>> decisions(
      @RequestParam(required = false) String orderId,
      @RequestParam(defaultValue = "100") int limit) {
    return jdbc.queryForList(
        "SELECT * FROM consumer_decision WHERE (?::text IS NULL OR order_id=?) ORDER BY id DESC LIMIT ?",
        orderId,
        orderId,
        Math.min(limit, 500));
  }

  @GetMapping("/outbox")
  public List<Map<String, Object>> outbox(
      @RequestParam(required = false) String orderId,
      @RequestParam(defaultValue = "100") int limit) {
    return jdbc.queryForList(
        "SELECT id,topic,aggregate_id,type,schema_version,correlation_id,causation_id,traceparent,"
            + "payload::text AS payload,created_at FROM outbox WHERE (?::text IS NULL OR aggregate_id=?)"
            + " ORDER BY created_at DESC LIMIT ?",
        orderId,
        orderId,
        Math.min(limit, 500));
  }

  @PostMapping("/consumers/{id}/{action}")
  public State consumer(@PathVariable String id, @PathVariable String action) {
    var container = registry.getListenerContainer(id);
    if (container == null)
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No consumer " + id);
    switch (action) {
      case "pause" -> container.pause();
      case "resume" -> container.resume();
      default -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "pause or resume");
    }
    return state();
  }

  @PutMapping("/faults/{name}")
  public State arm(
      @PathVariable String name,
      @RequestParam String mode,
      @RequestParam(required = false) Integer times) {
    faults.arm(name, mode, times);
    return state();
  }

  @DeleteMapping("/faults/{name}")
  public State clear(@PathVariable String name) {
    faults.clear(name);
    return state();
  }

  /** Answers first, then dies: the caller sees the request accepted, then the process vanish. */
  @PostMapping("/crash")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public void crash() {
    Thread.ofPlatform()
        .start(
            () -> {
              try {
                Thread.sleep(200);
              } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
              }
              Crash.now("operator requested a crash of " + service);
            });
  }
}
