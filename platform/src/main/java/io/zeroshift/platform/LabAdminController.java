package io.zeroshift.platform;

import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.platform.web.ApiException;
import io.zeroshift.platform.web.ApiResponse;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.*;

/**
 * The control plane's view into, and levers on, one service. Everything returned is read from the
 * live consumer containers or this service's database.
 */
@RestController
@RequestMapping("/lab")
public class LabAdminController {
  public static final String UNKNOWN_CONSUMER = "UNKNOWN_CONSUMER";
  public static final String UNKNOWN_CONSUMER_ACTION = "UNKNOWN_CONSUMER_ACTION";

  public record Consumer(
      String id,
      String groupId,
      List<String> topics,
      boolean running,
      boolean pauseRequested,
      boolean paused,
      List<String> assignedPartitions,
      Map<String, String> configOverrides) {}

  public record State(
      String service, List<Consumer> consumers, List<Faults.Fault> faults, Outbox.Status outbox) {}

  /** What an operator may do to one consumer. Stopping leaves the group; pausing does not. */
  enum ConsumerAction {
    PAUSE,
    RESUME,
    STOP,
    START
  }

  private final String service;
  private final KafkaListenerEndpointRegistry registry;
  private final Faults faults;
  private final DecisionLog decisions;
  private final Outbox outbox;
  private final LabPressure pressure;

  public LabAdminController(
      @Value("${spring.application.name}") String service,
      @Value("${zeroshift.instance:${HOSTNAME:local}}") String instance,
      KafkaListenerEndpointRegistry registry,
      Faults faults,
      DecisionLog decisions,
      Outbox outbox,
      MeterRegistry meters) {
    this.service = service;
    this.registry = registry;
    this.faults = faults;
    this.decisions = decisions;
    this.outbox = outbox;
    pressure = new LabPressure(meters, service, instance);
  }

  /** CPU, heap, threads, connection pool, rebalances and counters, read from this JVM now. */
  @GetMapping("/pressure")
  public LabPressure.Pressure pressure() {
    return pressure.read();
  }

  @GetMapping("/state")
  public State state() {
    var consumers =
        registry.getListenerContainers().stream()
            .map(LabAdminController::consumer)
            .sorted(Comparator.comparing(Consumer::id))
            .toList();
    return new State(service, consumers, faults.armed(), outbox.status());
  }

  @GetMapping("/decisions")
  public ApiResponse<List<DecisionLog.Recorded>> decisions(
      @RequestParam(required = false) String orderId,
      @RequestParam(defaultValue = "100") @Min(1) @Max(500) int limit) {
    return ApiResponse.page(decisions.recent(orderId, limit), limit);
  }

  @GetMapping("/outbox")
  public ApiResponse<List<Outbox.Row>> outbox(
      @RequestParam(required = false) String orderId,
      @RequestParam(defaultValue = "100") @Min(1) @Max(500) int limit) {
    return ApiResponse.page(outbox.recent(orderId, limit), limit);
  }

  @PostMapping("/consumers/{id}/{action}")
  public State consumer(@PathVariable String id, @PathVariable String action) {
    var container = container(id);
    var parsed =
        Arrays.stream(ConsumerAction.values())
            .filter(a -> a.name().equalsIgnoreCase(action))
            .findFirst()
            .orElseThrow(
                () ->
                    new ApiException(
                        HttpStatus.BAD_REQUEST,
                        UNKNOWN_CONSUMER_ACTION,
                        "Consumer actions are pause, resume, stop and start"));
    switch (parsed) {
      case PAUSE -> container.pause();
      case RESUME -> container.resume();
      case STOP -> container.stop();
      case START -> container.start();
    }
    return state();
  }

  /**
   * Overrides {@code max.poll.records} and {@code max.poll.interval.ms} for one consumer and
   * restarts it, so its consumers rejoin the group with the new settings. Without either parameter
   * the consumer factory's defaults apply again. Answers at once; the restart completes shortly
   * after (the returned state may still show the consumer stopping).
   */
  @PutMapping("/consumers/{id}/config")
  public State configure(
      @PathVariable String id,
      @RequestParam(required = false) @Min(1) @Max(10_000) Integer maxPollRecords,
      @RequestParam(required = false) @Min(1_000) @Max(600_000) Integer maxPollIntervalMs) {
    var container = container(id);
    var properties = container.getContainerProperties().getKafkaConsumerProperties();
    var before = new java.util.Properties();
    before.putAll(properties);
    set(properties, ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    set(properties, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
    if (properties.equals(before)) return state(); // nothing changed: no needless rebalance
    // Stopping waits for the record being handled and for the consumers to leave the group, which
    // can take seconds with a slow consumer: restart in the background and answer now.
    if (container.isRunning()) container.stop(container::start);
    else container.start();
    return state();
  }

  private static void set(java.util.Properties properties, String name, Integer value) {
    if (value == null) properties.remove(name);
    else properties.setProperty(name, value.toString());
  }

  private MessageListenerContainer container(String id) {
    var container = registry.getListenerContainer(id);
    if (container == null)
      throw new ApiException(HttpStatus.NOT_FOUND, UNKNOWN_CONSUMER, "No consumer " + id);
    return container;
  }

  /** Arms a fault; without {@code times} it stays armed until cleared. */
  @PutMapping("/faults/{name}")
  public State arm(
      @PathVariable @Size(max = 60) String name,
      @RequestParam @NotBlank @Size(max = 100) String mode,
      @RequestParam(required = false) @Min(1) @Max(1000) Integer times) {
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

  private static Consumer consumer(MessageListenerContainer c) {
    return new Consumer(
        c.getListenerId(),
        c.getGroupId(),
        Arrays.asList(
            Objects.requireNonNullElse(c.getContainerProperties().getTopics(), new String[0])),
        c.isRunning(),
        c.isPauseRequested(),
        c.isContainerPaused(),
        Objects.requireNonNullElse(c.getAssignedPartitions(), List.<TopicPartition>of()).stream()
            .map(TopicPartition::toString)
            .sorted()
            .toList(),
        c.getContainerProperties().getKafkaConsumerProperties().entrySet().stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    e -> e.getKey().toString(),
                    e -> e.getValue().toString(),
                    (a, b) -> b,
                    TreeMap::new)));
  }
}
