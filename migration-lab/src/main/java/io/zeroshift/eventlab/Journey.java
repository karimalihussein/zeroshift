package io.zeroshift.eventlab;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * One order's path through the system, message by message, joined from where each fact actually
 * lives: the producing service's outbox, the tapped Kafka record, every consumer's recorded
 * decisions, the saga's transitions, the event store and the read model. The join key is the event
 * id; retries and dead letters logged without one are matched by topic, partition and offset.
 */
@Component
public class Journey {
  private static final List<String> PRODUCERS =
      List.of("order-service", "payment-service", "inventory-service", "shipping-service");
  private static final List<String> CONSUMERS =
      List.of(
          "order-service",
          "payment-service",
          "inventory-service",
          "shipping-service",
          "order-query-service");

  private final LabServices services;
  private final JdbcTemplate jdbc;
  private final JsonMapper json = JsonMapper.builder().build();

  public Journey(LabServices services, JdbcTemplate jdbc) {
    this.services = services;
    this.jdbc = jdbc;
  }

  public ObjectNode of(UUID orderId) {
    var result = json.createObjectNode();
    var order = services.tryGet("order-service", "/orders/" + orderId);
    result.set("order", order);
    result.set("readModel", services.tryGet("order-query-service", "/orders/" + orderId));
    result.set(
        "gatewayCalls",
        services.tryGet("payment-service", "/lab/gateway/calls?orderId=" + orderId));

    var messages = new LinkedHashMap<String, ObjectNode>();
    for (var service : PRODUCERS)
      for (var row :
          array(services.tryGet(service, "/lab/outbox?orderId=" + orderId + "&limit=200"))) {
        var m = message(messages, row.path("id").asString());
        m.put("producer", service);
        m.put("type", row.path("type").asString());
        m.put("topic", row.path("topic").asString());
        m.put("schemaVersion", row.path("schema_version").asInt());
        m.put("correlationId", row.path("correlation_id").asString(null));
        m.put("causationId", row.path("causation_id").asString(null));
        m.put("traceparent", row.path("traceparent").asString(null));
        m.put("outboxAt", row.path("created_at").asString());
        m.set("envelope", parse(row.path("payload").asString()));
      }

    var tapped =
        jdbc.queryForList(
            "SELECT topic,kafka_partition,kafka_offset,event_id,type,headers::text AS headers,value,kafka_timestamp,tapped_at"
                + " FROM event_tap WHERE record_key=? ORDER BY kafka_timestamp",
            orderId.toString());
    var byPosition = new HashMap<String, ObjectNode>();
    var ghosts = json.createArrayNode();
    for (var row : tapped) {
      var kafka = json.createObjectNode();
      kafka.put("topic", (String) row.get("topic"));
      kafka.put("partition", (Integer) row.get("kafka_partition"));
      kafka.put("offset", (Long) row.get("kafka_offset"));
      // The record timestamp is set by Debezium when it read the change from the WAL; it keeps
      // reading while the connector is paused. tappedAt is when the control plane's own consumer
      // actually received it from Kafka (at most one poll late).
      kafka.put(
          "timestamp", ((java.sql.Timestamp) row.get("kafka_timestamp")).toInstant().toString());
      kafka.put("tappedAt", ((java.sql.Timestamp) row.get("tapped_at")).toInstant().toString());
      kafka.set("headers", parse((String) row.get("headers")));
      var eventId = row.get("event_id") == null ? null : row.get("event_id").toString();
      byPosition.put(
          position(row.get("topic"), row.get("kafka_partition"), row.get("kafka_offset")),
          eventId == null ? null : message(messages, eventId));
      if (eventId == null) {
        kafka.put("value", (String) row.get("value"));
        ghosts.add(kafka);
        continue;
      }
      var m = message(messages, eventId);
      if (!m.has("type")) m.put("type", (String) row.get("type"));
      if (!m.has("envelope")) m.set("envelope", parse((String) row.get("value")));
      ((ArrayNode) m.withArray("kafka")).add(kafka);
    }

    for (var service : CONSUMERS)
      for (var row :
          array(services.tryGet(service, "/lab/decisions?orderId=" + orderId + "&limit=500"))) {
        var eventId = row.path("event_id").asString(null);
        var target =
            eventId != null
                ? message(messages, eventId)
                : byPosition.get(
                    position(
                        row.path("topic").asString(),
                        row.path("kafka_partition").asInt(),
                        row.path("kafka_offset").asLong()));
        var delivery = json.createObjectNode();
        delivery.put("service", service);
        delivery.put("consumer", row.path("consumer").asString());
        delivery.put("decision", row.path("decision").asString());
        delivery.put("attempt", row.path("attempt").asInt());
        delivery.put("detail", row.path("detail").asString());
        delivery.put("topic", row.path("topic").asString());
        delivery.put("partition", row.path("kafka_partition").asInt());
        delivery.put("offset", row.path("kafka_offset").asLong());
        delivery.put("traceId", row.path("trace_id").asString(null));
        delivery.put("at", row.path("at").asString());
        if (target == null) ghosts.add(delivery);
        else ((ArrayNode) target.withArray("deliveries")).add(delivery);
      }

    for (var t : array(order.path("transitions")))
      if (t.hasNonNull("trigger_event_id"))
        Optional.ofNullable(messages.get(t.path("trigger_event_id").asString()))
            .ifPresent(m -> m.set("sagaTransition", t));
    for (var e : array(order.path("events")))
      Optional.ofNullable(messages.get(e.path("eventId").asString()))
          .ifPresent(
              m -> {
                m.put("eventStorePosition", e.path("position").asLong());
                m.put("eventStoreVersion", e.path("version").asLong());
                m.put("recordedAt", e.path("recordedAt").asString());
              });

    var ordered = new ArrayList<>(messages.values());
    ordered.sort(Comparator.comparing(Journey::firstSeen));
    var list = result.putArray("messages");
    ordered.forEach(list::add);
    result.set("unmatched", ghosts);
    return result;
  }

  private static java.time.Instant firstSeen(ObjectNode m) {
    var kafka = m.path("kafka");
    var text =
        m.hasNonNull("outboxAt")
            ? m.get("outboxAt").asString()
            : kafka.isEmpty() ? null : kafka.get(0).path("timestamp").asString();
    return text == null ? java.time.Instant.MAX : java.time.OffsetDateTime.parse(text).toInstant();
  }

  private ObjectNode message(Map<String, ObjectNode> messages, String eventId) {
    return messages.computeIfAbsent(eventId, id -> json.createObjectNode().put("eventId", id));
  }

  private static String position(Object topic, Object partition, Object offset) {
    return topic + "-" + partition + "@" + offset;
  }

  private JsonNode parse(String text) {
    try {
      return text == null ? null : json.readTree(text);
    } catch (RuntimeException e) {
      return json.getNodeFactory().stringNode(text);
    }
  }

  private static Iterable<JsonNode> array(JsonNode node) {
    return node != null && node.isArray() ? node : List.of();
  }
}
