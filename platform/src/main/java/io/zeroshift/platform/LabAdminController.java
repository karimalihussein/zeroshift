package io.zeroshift.platform;

import static io.zeroshift.platform.db.Tables.CONSUMER_DECISION;
import static io.zeroshift.platform.db.Tables.OUTBOX;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.max;

import java.util.*;
import org.apache.kafka.common.TopicPartition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
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

  /**
   * The service's side of change data capture. {@code lagBytes} is WAL the connector has not yet
   * confirmed: outbox inserts committed here but not yet on Kafka (plus unrelated WAL traffic).
   */
  public record Outbox(
      String slot, boolean slotActive, Long lagBytes, long rows, Object lastInsertAt) {}

  public record State(
      String service, List<Consumer> consumers, List<Map<String, Object>> faults, Outbox outbox) {}

  private final String service;
  private final KafkaListenerEndpointRegistry registry;
  private final Faults faults;
  private final DSLContext db;

  public LabAdminController(
      @Value("${spring.application.name}") String service,
      KafkaListenerEndpointRegistry registry,
      Faults faults,
      DSLContext db) {
    this.service = service;
    this.registry = registry;
    this.faults = faults;
    this.db = db;
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
    return new State(service, consumers, faults.armed(), outbox());
  }

  private Outbox outbox() {
    // A system view, not our schema: plain SQL. lag = WAL the connector has not yet confirmed.
    var slot =
        db.fetchOptional(
            "SELECT slot_name, active,"
                + " pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint AS lag"
                + " FROM pg_replication_slots WHERE slot_name = current_database() || '_outbox'");
    var table = db.select(count(), max(OUTBOX.CREATED_AT)).from(OUTBOX).fetchSingle();
    return new Outbox(
        slot.map(r -> r.get("slot_name", String.class)).orElse(null),
        slot.map(r -> r.get("active", Boolean.class)).orElse(false),
        slot.map(r -> r.get("lag", Long.class)).orElse(null),
        table.value1(),
        table.value2());
  }

  @GetMapping("/decisions")
  public List<Map<String, Object>> decisions(
      @RequestParam(required = false) String orderId,
      @RequestParam(defaultValue = "100") int limit) {
    return db.selectFrom(CONSUMER_DECISION)
        .where(orderId == null ? DSL.noCondition() : CONSUMER_DECISION.ORDER_ID.eq(orderId))
        .orderBy(CONSUMER_DECISION.ID.desc())
        .limit(Math.min(limit, 500))
        .fetchMaps();
  }

  @GetMapping("/outbox")
  public List<Map<String, Object>> outbox(
      @RequestParam(required = false) String orderId,
      @RequestParam(defaultValue = "100") int limit) {
    return db.select(
            OUTBOX.ID,
            OUTBOX.TOPIC,
            OUTBOX.AGGREGATE_ID,
            OUTBOX.TYPE,
            OUTBOX.SCHEMA_VERSION,
            OUTBOX.CORRELATION_ID,
            OUTBOX.CAUSATION_ID,
            OUTBOX.TRACEPARENT,
            OUTBOX.PAYLOAD.cast(String.class).as("payload"),
            OUTBOX.CREATED_AT)
        .from(OUTBOX)
        .where(orderId == null ? DSL.noCondition() : OUTBOX.AGGREGATE_ID.eq(orderId))
        .orderBy(OUTBOX.CREATED_AT.desc())
        .limit(Math.min(limit, 500))
        .fetchMaps();
  }

  @PostMapping("/consumers/{id}/{action}")
  public State consumer(@PathVariable String id, @PathVariable String action) {
    var container = registry.getListenerContainer(id);
    if (container == null)
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No consumer " + id);
    switch (action) {
      case "pause" -> container.pause();
      case "resume" -> container.resume();
      // Stopping leaves the consumer group (pausing does not): needed before offsets can move.
      case "stop" -> container.stop();
      case "start" -> container.start();
      default ->
          throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "pause, resume, stop or start");
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
