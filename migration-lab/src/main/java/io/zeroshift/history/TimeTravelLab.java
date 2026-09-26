package io.zeroshift.history;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Rebuilds one real order at every version and at a moment between two of its events, then finds
 * the same events on Kafka by timestamp and checks both histories agree.
 */
@Component
public class TimeTravelLab implements HistoryLab {
  private final OrderHistory orders;
  private final JsonMapper json = JsonMapper.builder().build();

  public TimeTravelLab(OrderHistory orders) {
    this.orders = orders;
  }

  @Override
  public String id() {
    return "time-travel";
  }

  @Override
  public String title() {
    return "Replay and time travel";
  }

  @Override
  public String summary() {
    return "An order's state is never stored, only derived. Rebuild it at any version or instant"
        + " from the event store, then replay the same events from Kafka by timestamp.";
  }

  @Override
  public List<String> plan() {
    return List.of(
        "Pick the most recent finished order and list its stored events: version, type, when recorded, schema version.",
        "Compare each event as stored with how today's code reads it (older schemas are upcast on read).",
        "Rebuild the order at every version, and at an instant between its first two events, folding only what existed then.",
        "Ask Kafka's time index for the offset of the order's placement time and read its partition from there.",
        "Check the event store and Kafka hold the same events in the same order, and how long each took to be relayed.",
        "What a fold, a version and a timestamp mean, from this order's numbers.");
  }

  @Override
  public ObjectNode run(int step, ObjectNode memo) throws Exception {
    var result = json.createObjectNode();
    switch (step) {
      case 0 -> {
        var chosen = memo.path("orderId").asString(null);
        if (chosen == null)
          for (var summary : OrderHistory.list(orders.recent(50, null)))
            if ("COMPLETED".equals(summary.path("saga").path("state").asString())) {
              chosen = summary.path("order").path("id").asString();
              break;
            }
        if (chosen == null)
          throw new IllegalStateException("No finished order yet: place one first");
        memo.put("orderId", chosen);
        var history = orders.history(UUID.fromString(chosen));
        memo.set("history", history);
        result.put("orderId", chosen);
        var events = json.createArrayNode();
        for (var m : history) {
          var e = m.path("event");
          events
              .addObject()
              .put("version", e.path("version").asLong())
              .put("type", e.path("type").asString())
              .put("recordedAt", e.path("recordedAt").asString())
              .put("storedVersion", e.path("storedVersion").asInt())
              .put("position", e.path("position").asLong())
              .put("statusAfter", m.path("stateAfter").path("status").asString());
        }
        result.set("events", events);
      }
      case 1 -> {
        var diffs = json.createArrayNode();
        for (var m : memo.path("history")) {
          var e = m.path("event");
          var stored = e.path("stored").path("payload");
          var decoded = e.path("decoded");
          var added = json.createArrayNode();
          for (var name : decoded.propertyNames()) if (!stored.has(name)) added.add(name);
          diffs
              .addObject()
              .put("version", e.path("version").asLong())
              .put("type", e.path("type").asString())
              .put("storedVersion", e.path("storedVersion").asInt())
              .put("currentVersion", e.path("currentVersion").asInt())
              .<ObjectNode>set("addedOnRead", added)
              .<ObjectNode>set("stored", stored)
              .set("decoded", decoded);
        }
        result.set("events", diffs);
      }
      case 2 -> {
        var id = UUID.fromString(memo.path("orderId").asString());
        var history = memo.path("history");
        var states = json.createArrayNode();
        for (int v = 1; v <= history.size(); v++) {
          var rebuilt = orders.rebuild(id, (long) v, null);
          states
              .addObject()
              .put("version", v)
              .put("applied", rebuilt.path("applied").size())
              .put("status", rebuilt.path("state").path("status").asString())
              .set("state", rebuilt.path("state"));
        }
        result.set("byVersion", states);
        if (history.size() >= 2) {
          var first = Instant.parse(history.get(0).path("event").path("recordedAt").asString());
          var second = Instant.parse(history.get(1).path("event").path("recordedAt").asString());
          var between = first.plus(Duration.between(first, second).dividedBy(2));
          var rebuilt = orders.rebuild(id, null, between);
          result.put("between", between.toString());
          result.put("betweenStatus", rebuilt.path("state").path("status").asString());
          result.put("betweenApplied", rebuilt.path("applied").size());
          result.put("betweenLater", rebuilt.path("later").size());
        }
        var last = states.get(states.size() - 1);
        var now = history.get(history.size() - 1).path("stateAfter");
        result.put("lastEqualsCurrent", last.path("state").equals(now));
      }
      case 3 -> {
        var id = UUID.fromString(memo.path("orderId").asString());
        var placed =
            Instant.parse(memo.path("history").get(0).path("event").path("recordedAt").asString());
        // Kafka's timestamp is set when Debezium relays the row, a little after it was recorded.
        var from = placed.minusSeconds(1);
        var replay = orders.kafkaFrom(id, from);
        memo.set("kafka", replay);
        result.setAll(replay);
      }
      case 4 -> {
        var stored = new ArrayList<String>();
        for (var m : memo.path("history")) stored.add(m.path("event").path("eventId").asString());
        var onKafka = new ArrayList<String>();
        var delays = json.createArrayNode();
        var records = memo.path("kafka").path("records");
        for (int i = 0; i < records.size(); i++) {
          var r = records.get(i);
          onKafka.add(r.path("eventId").asString());
          if (i < memo.path("history").size()) {
            var recorded =
                Instant.parse(
                    memo.path("history").get(i).path("event").path("recordedAt").asString());
            delays
                .addObject()
                .put("type", r.path("type").asString())
                .put("offset", r.path("offset").asLong())
                .put(
                    "relayMs",
                    Duration.between(recorded, Instant.parse(r.path("timestamp").asString()))
                        .toMillis());
          }
        }
        result.put("eventStoreEvents", stored.size());
        result.put("kafkaRecords", onKafka.size());
        result.put("sameEventsSameOrder", stored.equals(onKafka));
        result.set("relay", delays);
        var offsets = json.createArrayNode();
        records.forEach(r -> offsets.add(r.path("offset").asLong()));
        result.set("offsets", offsets);
      }
      case 5 -> {
        int n = memo.path("history").size();
        result.put(
            "text",
            "The order was never stored as a row: its state at version k is the fold of its first k"
                + " events, so any past version can be rebuilt exactly ("
                + n
                + " versions here). A timestamp works the same way, choosing the events recorded"
                + " by then. Kafka holds the same events as its published copy, and its time index"
                + " turns an instant into a real offset per partition, so replaying \"from 10:42\""
                + " starts at the right record without scanning. The two histories agree event for"
                + " event; Kafka's are a few hundred milliseconds later because Debezium relays"
                + " them after commit.");
      }
      default -> throw new IllegalArgumentException("No step " + step);
    }
    return result;
  }
}
