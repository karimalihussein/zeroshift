package io.zeroshift.history;

import io.zeroshift.contracts.Contracts;
import io.zeroshift.contracts.Topics;
import io.zeroshift.eventlab.LabServices;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Schema evolution on the running system: an order written by the previous release (OrderPlaced v1)
 * read by today's services, an event from a release not yet rolled out (v3) meeting today's
 * readers, and every reader against every writer.
 */
@Component
public class SchemaLab implements HistoryLab {
  private static final String DLT = Topics.deadLetter(Topics.ORDER_EVENTS);

  private final LabServices services;
  private final OrderHistory orders;
  private final KafkaHistory kafka;
  private final SkuSalesProjection projection;
  private final JsonMapper json = JsonMapper.builder().build();

  public SchemaLab(
      LabServices services,
      OrderHistory orders,
      KafkaHistory kafka,
      SkuSalesProjection projection) {
    this.services = services;
    this.orders = orders;
    this.kafka = kafka;
    this.projection = projection;
  }

  @Override
  public String id() {
    return "schema";
  }

  @Override
  public String title() {
    return "Schema evolution";
  }

  @Override
  public String summary() {
    return "OrderPlaced has two versions in production and a proposed third. Old events meet new"
        + " readers, a new writer meets old readers, and a dead letter explains why it failed.";
  }

  @Override
  public List<String> plan() {
    return List.of(
        "Count stored events by type and schema version, and read the contract registry: current versions, upcasters, downcasters.",
        "Place an order as the previous release wrote it (OrderPlaced v1, no currency) and follow it: stored as v1, read everywhere through the v1 → v2 upcaster.",
        "A release that is not rolled out yet writes OrderPlaced v3 (money as integer cents) to order.events.",
        "Watch the deployed reader (order-projection) refuse it at once and dead-letter it, and find the dead letter with its reason.",
        "Run every reader (v1, v2 deployed, v2 without its version check, v3 proposed) against a real v1, v2 and v3 record.",
        "Which rollout order works, and why the version check is what makes a failure clear.");
  }

  @Override
  public ObjectNode run(int step, ObjectNode memo) throws Exception {
    var result = json.createObjectNode();
    switch (step) {
      case 0 -> {
        result.set(
            "stored", services.tryGetAnyReplica(OrderHistory.ORDERS, "/lab/history/versions"));
        var contracts = json.createArrayNode();
        for (var c : Contracts.all())
          contracts
              .addObject()
              .put("type", c.type())
              .put("topic", c.topic())
              .put("version", c.version())
              .put(
                  "upcasters",
                  c.upcasters().keySet().stream()
                      .sorted()
                      .map(v -> "v" + v + " → v" + (v + 1))
                      .toList()
                      .toString())
              .put(
                  "downcasters",
                  c.downcasters().keySet().stream()
                      .sorted()
                      .map(v -> "v" + v + " → v" + (v - 1))
                      .toList()
                      .toString());
        result.set("contracts", contracts);
      }
      case 1 -> {
        var since = Instant.now().minusSeconds(2);
        var placed =
            services.post(
                OrderHistory.ORDERS,
                "/lab/history/legacy-order",
                Map.of(
                    "customerId",
                    "legacy-client",
                    "items",
                    List.of(Map.of("sku", "SKU-CABLE", "quantity", 1))));
        var id = UUID.fromString(placed.path("orderId").asString());
        memo.put("legacyOrder", id.toString());
        memo.put("legacySince", since.toString());
        var history = orders.history(id);
        var first = history.get(0).path("event");
        result.put("orderId", id.toString());
        result.put("storedVersion", first.path("storedVersion").asInt());
        result.set("stored", first.path("stored").path("payload"));
        result.put("currentVersion", first.path("currentVersion").asInt());
        result.set("decoded", first.path("decoded"));
        var record = awaitRecord(Topics.ORDER_EVENTS, id, "OrderPlaced", since);
        result.put("kafkaOffset", record == null ? -1 : record.offset());
        result.put(
            "kafkaSchemaVersion",
            record == null ? 0 : json.readTree(record.value()).path("schemaVersion").asInt());
        result.set("kafkaHeaders", record == null ? null : json.valueToTree(record.headers()));
        result.put("saga", awaitSaga(id));
        // The read model was built by another service from the v1 record on Kafka.
        result.set("readModel", services.tryGet("order-query-service", "/orders/" + id));
      }
      case 2 -> {
        var legacy = UUID.fromString(memo.path("legacyOrder").asString());
        var template =
            (ObjectNode) orders.history(legacy).get(0).path("event").path("stored").deepCopy();
        // A new order id and event id: the event stands alone, it touches no existing order.
        var id = UUID.randomUUID();
        var v2 = template;
        v2.put("eventId", UUID.randomUUID().toString());
        v2.put("schemaVersion", 2);
        var payload = (ObjectNode) v2.get("payload");
        payload.put("orderId", id.toString());
        payload.put("customerId", "canary-release");
        if (!payload.has("currency")) payload.put("currency", "USD");
        var v3 = SchemaReaders.asV3(v2);
        var since = Instant.now().minusSeconds(1);
        var meta = kafka.send(Topics.ORDER_EVENTS, id.toString(), v3.toString());
        memo.put("v3Order", id.toString());
        memo.put("v3Since", since.toString());
        memo.put("v3Record", v3.toString());
        result.put("orderId", id.toString());
        result.put("partition", meta.partition());
        result.put("offset", meta.offset());
        result.set("written", v3);
      }
      case 3 -> {
        var id = memo.path("v3Order").asString();
        var since = Instant.parse(memo.path("v3Since").asString());
        ObjectNode decision = null;
        long deadline = System.currentTimeMillis() + 60_000;
        while (decision == null && System.currentTimeMillis() < deadline) {
          for (var d :
              LabServices.data(
                  services.tryGet("order-query-service", "/lab/decisions?orderId=" + id)))
            if (d.path("decision").asString().equals("DEAD_LETTERED")) decision = (ObjectNode) d;
          if (decision == null) Thread.sleep(1000);
        }
        result.set("decision", decision);
        var dead = awaitRecord(DLT, UUID.fromString(id), null, since);
        if (dead != null) {
          result.put("dltTopic", DLT);
          result.put("dltOffset", dead.offset());
          var h = dead.headers();
          result.put("exception", h.get("kafka_dlt-exception-fqcn"));
          result.put("reason", h.get("kafka_dlt-exception-message"));
          result.put("originalTopic", h.get("kafka_dlt-original-topic"));
          result.put("originalOffset", h.get("kafka_dlt-original-offset"));
        }
        projection.catchUp(SkuSalesProjection.V2);
        result.set(
            "labProjectionSkipped",
            json.valueToTree(
                projection.skipped(SkuSalesProjection.V2).stream()
                    .filter(s -> s.reason().contains("v3"))
                    .toList()));
      }
      case 4 -> {
        var legacy =
            orders.find(
                Topics.ORDER_EVENTS,
                UUID.fromString(memo.path("legacyOrder").asString()),
                "OrderPlaced",
                Instant.parse(memo.path("legacySince").asString()));
        var current = currentV2Record();
        var rows = json.createArrayNode();
        addRow(rows, "v1 (previous release)", legacy == null ? null : legacy.value());
        addRow(rows, "v2 (current release)", current == null ? null : current.value());
        addRow(rows, "v3 (not rolled out)", memo.path("v3Record").asString(null));
        result.set("matrix", rows);
        var safe = json.createObjectNode();
        for (var row : rows) {
          var readers = json.createArrayNode();
          for (var r : row.path("readings"))
            if (r.path("read").asBoolean()) readers.add(r.path("reader").asString());
          safe.set(row.path("written").asString(), readers);
        }
        result.set("readableBy", safe);
      }
      case 5 ->
          result.put(
              "text",
              "Backward compatibility (a new reader, an old event) is the upcaster's job: v1 events"
                  + " are stored and published as v1 forever, and every reader turns them into v2 on"
                  + " the way in. Forward compatibility (an old reader, a new event) is impossible"
                  + " for a breaking change: the deployed v2 reader cannot know what unitPriceMinor"
                  + " means. Its version check makes that explicit (MalformedMessageException, never"
                  + " retried, dead-lettered on the first delivery, with the reason in the"
                  + " dead letter's headers). Without the check the same reader fails anyway, but"
                  + " with a misleading message (a price \"must not be negative\") or, worse, reads"
                  + " a wrong value. So a breaking change rolls out readers first: deploy v3-capable"
                  + " readers everywhere (they read v1, v2 and v3), then switch writers to v3, then"
                  + " retire v2 writing. Additive changes need no version bump at all: tolerant"
                  + " readers ignore fields they do not know.");
      default -> throw new IllegalArgumentException("No step " + step);
    }
    return result;
  }

  private void addRow(tools.jackson.databind.node.ArrayNode rows, String written, String record) {
    var row = rows.addObject();
    row.put("written", written);
    if (record == null) {
      row.put("missing", true);
      return;
    }
    row.set("record", json.readTree(record).path("payload"));
    row.set("readings", json.valueToTree(SchemaReaders.readAll(record)));
  }

  /** The newest OrderPlaced written by the current release, from Kafka. */
  private KafkaHistory.Record currentV2Record() throws Exception {
    for (var summary : OrderHistory.list(orders.recent(20, null))) {
      var id = UUID.fromString(summary.path("order").path("id").asString());
      var history = orders.history(id);
      var first = history.get(0).path("event");
      if (first.path("storedVersion").asInt() != 2) continue;
      var recorded = Instant.parse(first.path("recordedAt").asString());
      var record = orders.find(Topics.ORDER_EVENTS, id, "OrderPlaced", recorded.minusSeconds(1));
      if (record != null) return record;
    }
    return null;
  }

  private KafkaHistory.Record awaitRecord(String topic, UUID order, String type, Instant since)
      throws Exception {
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      var record = orders.find(topic, order, type, since);
      if (record != null) return record;
      Thread.sleep(500);
    }
    return null;
  }

  private String awaitSaga(UUID order) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 60_000;
    String state = "";
    while (System.currentTimeMillis() < deadline) {
      state =
          services
              .tryGetAnyReplica(OrderHistory.ORDERS, "/orders/" + order)
              .path("saga")
              .path("state")
              .asString("");
      if (state.equals("COMPLETED") || state.equals("CANCELLED")) return state;
      Thread.sleep(1000);
    }
    return state;
  }
}
