package io.zeroshift.history;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * One compacted topic, {@code lab.order-status}: the status history of real orders, keyed by order
 * id. Kafka's log cleaner keeps the latest value per key and drops the rest; a null value (a
 * tombstone) deletes a key, and is itself removed once {@code delete.retention.ms} has passed.
 * Compared with {@code lab.retention-demo}, which deletes whole segments by age whatever their
 * keys.
 */
@Component
public class CompactionLab implements HistoryLab {
  public static final String TOPIC = "lab.order-status";
  static final String RETENTION = "lab.retention-demo";

  /**
   * Lab-sized cleaning: segments roll every 5 s, any dirty segment is worth cleaning, and a
   * tombstone survives 20 s after its segment was cleaned. Production defaults are 7 days, 50 %
   * dirty and 24 h.
   */
  static final Map<String, String> CONFIG =
      Map.of(
          "cleanup.policy", "compact",
          "segment.ms", "5000",
          "min.cleanable.dirty.ratio", "0.01",
          "min.compaction.lag.ms", "0",
          "max.compaction.lag.ms", "10000",
          "delete.retention.ms", "20000");

  private final KafkaHistory kafka;
  private final OrderHistory orders;
  private final JsonMapper json = JsonMapper.builder().build();

  public CompactionLab(KafkaHistory kafka, OrderHistory orders) {
    this.kafka = kafka;
    this.orders = orders;
  }

  @Override
  public String id() {
    return "compaction";
  }

  @Override
  public String title() {
    return "Log compaction";
  }

  @Override
  public String summary() {
    return "A compacted topic keeps the latest value for every key forever and forgets the rest;"
        + " retention forgets everything older than a deadline. Tombstones delete keys.";
  }

  @Override
  public List<String> plan() {
    return List.of(
        "Recreate lab.order-status (compacted, 1 partition) and write the status history of five real orders, one record per status change, keyed by order id; then a tombstone for the first order.",
        "Read the whole topic from offset 0: every status of every order is still there.",
        "Roll the active segment so the log cleaner may compact it, and wait until it has.",
        "Replay the topic from offset 0 again, as a new consumer rebuilding a table would.",
        "Compare before and after per key, check the rebuilt table against order-service, and contrast with lab.retention-demo (time-based deletion).",
        "When compaction is the right tool, and what it does not guarantee.");
  }

  @Override
  public ObjectNode run(int step, ObjectNode memo) throws Exception {
    var result = json.createObjectNode();
    switch (step) {
      case 0 -> {
        recreate();
        var written = json.createArrayNode();
        var keys = json.createArrayNode();
        for (var summary : OrderHistory.list(orders.recent(15, null))) {
          if (keys.size() == 5) break;
          var id = summary.path("order").path("id").asString();
          var history = orders.history(UUID.fromString(id));
          if (history.size() < 2) continue;
          keys.add(id);
          for (var m : history) {
            var value = json.createObjectNode();
            value.put("status", m.path("stateAfter").path("status").asString());
            value.put("version", m.path("event").path("version").asLong());
            value.put("event", m.path("event").path("type").asString());
            var meta = kafka.send(TOPIC, id, value.toString());
            written.addObject().put("offset", meta.offset()).put("key", id).set("value", value);
          }
        }
        var forgotten = keys.get(0).asString();
        var meta = kafka.send(TOPIC, forgotten, null);
        written.addObject().put("offset", meta.offset()).put("key", forgotten).putNull("value");
        memo.set("keys", keys);
        memo.put("tombstoned", forgotten);
        result.put("topic", TOPIC);
        result.set("config", json.valueToTree(config(TOPIC)));
        result.set("written", written);
      }
      case 1 -> {
        var before = readAll();
        memo.set("before", before);
        result.set("records", before);
        result.put("count", before.size());
        result.set("spans", json.valueToTree(kafka.spans(TOPIC, null)));
      }
      case 2 -> {
        // A segment is only cleaned once it is no longer the active one: a record written after
        // segment.ms rolls it. Then wait for the cleaner (it checks every 15 s by default).
        Thread.sleep(6000);
        kafka.send(TOPIC, "segment-roll", "{\"note\":\"written only to roll the segment\"}");
        int before = memo.path("before").size();
        long started = System.currentTimeMillis();
        int now = before;
        while (System.currentTimeMillis() - started < 120_000) {
          now = readAll().size();
          if (now < before) break;
          Thread.sleep(3000);
        }
        result.put("recordsBefore", before);
        result.put("recordsAfter", now);
        result.put("waitedMs", System.currentTimeMillis() - started);
        result.put("compacted", now < before);
        result.set("spans", json.valueToTree(kafka.spans(TOPIC, null)));
      }
      case 3 -> {
        var after = readAll();
        memo.set("after", after);
        result.set("records", after);
        result.put("count", after.size());
        // What a consumer rebuilding a table from the topic ends up with.
        var table = new TreeMap<String, String>();
        for (var r : after)
          if (r.path("value").isNull()) table.remove(r.path("key").asString());
          else table.put(r.path("key").asString(), r.path("value").path("status").asString(null));
        table.remove("segment-roll");
        memo.set("table", json.valueToTree(table));
        result.set("table", json.valueToTree(table));
      }
      case 4 -> {
        var perKey = json.createArrayNode();
        for (var key : memo.path("keys")) {
          var k = key.asString();
          var before = new ArrayList<String>();
          var after = new ArrayList<String>();
          for (var r : memo.path("before"))
            if (k.equals(r.path("key").asString())) before.add(label(r));
          for (var r : memo.path("after"))
            if (k.equals(r.path("key").asString())) after.add(label(r));
          var current =
              orders
                  .rebuild(UUID.fromString(k), null, null)
                  .path("state")
                  .path("status")
                  .asString();
          perKey
              .addObject()
              .put("key", k)
              .put("tombstoned", k.equals(memo.path("tombstoned").asString()))
              .<ObjectNode>set("before", json.valueToTree(before))
              .<ObjectNode>set("after", json.valueToTree(after))
              .put("orderServiceStatus", current)
              .put("tableStatus", memo.path("table").path(k).asString(null));
        }
        result.set("perKey", perKey);
        var retention = json.createObjectNode();
        retention.set("config", json.valueToTree(config(RETENTION)));
        retention.set("spans", json.valueToTree(kafka.spans(RETENTION, null)));
        result.set("retention", retention);
        // Offsets are never renumbered: compaction leaves gaps.
        var offsets = json.createArrayNode();
        for (var r : memo.path("after")) offsets.add(r.path("offset").asLong());
        result.set("offsetsAfter", offsets);
      }
      case 5 ->
          result.put(
              "text",
              "Compaction turns a topic into a durable table: whatever the retention, the latest"
                  + " value of every key stays, so a new consumer can rebuild the current state by"
                  + " reading from offset 0 (Kafka Connect's own offsets and configs live in"
                  + " compacted topics). It is not history: the intermediate statuses are gone, so"
                  + " order.events, the replayable history, is not compacted. Offsets are never"
                  + " renumbered, so a compacted log has gaps. A tombstone (a null value) deletes"
                  + " a key, and stays long enough (delete.retention.ms) for every consumer to see"
                  + " the delete. Retention (lab.retention-demo) instead drops whole segments once"
                  + " they are old, latest values included. Compaction runs in the background and"
                  + " only on inactive segments: a consumer may still see several values for a key.");
      default -> throw new IllegalArgumentException("No step " + step);
    }
    return result;
  }

  private static String label(tools.jackson.databind.JsonNode r) {
    return "@"
        + r.path("offset").asLong()
        + " "
        + (r.path("value").isNull() ? "tombstone" : r.path("value").path("status").asString());
  }

  private tools.jackson.databind.node.ArrayNode readAll() throws Exception {
    var records = kafka.read(TOPIC, kafka.spans(TOPIC, null), r -> true, 10_000);
    var result = json.createArrayNode();
    for (var r : records) {
      var node =
          result
              .addObject()
              .put("offset", r.offset())
              .put("key", r.key())
              .put("timestamp", r.timestamp().toString());
      if (r.value() == null) node.putNull("value");
      else node.set("value", json.readTree(r.value()));
    }
    return result;
  }

  /** Deletes the topic if it exists and creates it again, empty, with the lab's settings. */
  private void recreate() throws Exception {
    var admin = kafka.admin();
    try {
      admin.deleteTopics(List.of(TOPIC)).all().get(10, TimeUnit.SECONDS);
    } catch (java.util.concurrent.ExecutionException e) {
      if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) throw e;
    }
    long deadline = System.currentTimeMillis() + 30_000;
    while (true) {
      try {
        admin
            .createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1).configs(CONFIG)))
            .all()
            .get(10, TimeUnit.SECONDS);
        return;
      } catch (java.util.concurrent.ExecutionException e) {
        // Deletion is asynchronous: the old topic may still be going away.
        if (System.currentTimeMillis() > deadline) throw e;
        Thread.sleep(500);
      }
    }
  }

  private Map<String, String> config(String topic) throws Exception {
    var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    var config =
        kafka
            .admin()
            .describeConfigs(List.of(resource))
            .all()
            .get(10, TimeUnit.SECONDS)
            .get(resource);
    var result = new LinkedHashMap<String, String>();
    for (var name :
        List.of(
            "cleanup.policy",
            "retention.ms",
            "segment.ms",
            "min.cleanable.dirty.ratio",
            "max.compaction.lag.ms",
            "delete.retention.ms")) result.put(name, config.get(name).value());
    return result;
  }
}
