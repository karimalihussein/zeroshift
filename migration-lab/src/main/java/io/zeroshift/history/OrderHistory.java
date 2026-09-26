package io.zeroshift.history;

import io.zeroshift.eventlab.LabServices;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * An order's history from both places it lives: order-service's event store (the source of truth,
 * rebuilt at any version or time) and order.events on Kafka (its publication, replayable from any
 * timestamp by real offsets).
 */
@Component
public class OrderHistory {
  static final String ORDERS = "order-service";

  private final LabServices services;
  private final KafkaHistory kafka;
  private final JsonMapper json = JsonMapper.builder().build();

  public OrderHistory(LabServices services, KafkaHistory kafka) {
    this.services = services;
    this.kafka = kafka;
  }

  public JsonNode recent(int limit, String customer) {
    return LabServices.data(
        services.tryGetAnyReplica(
            ORDERS,
            "/orders?limit=" + limit + (customer == null ? "" : "&customerId=" + customer)));
  }

  public JsonNode history(UUID order) {
    return services.tryGetAnyReplica(ORDERS, "/orders/" + order + "/history");
  }

  public JsonNode rebuild(UUID order, Long version, Instant at) {
    var query = new StringBuilder("/orders/" + order + "/rebuild?");
    if (version != null) query.append("version=").append(version).append('&');
    if (at != null) query.append("at=").append(at);
    return services.tryGetAnyReplica(ORDERS, query.toString());
  }

  /**
   * order.events from {@code at}: per partition, the offset the broker's time index gives for that
   * instant and how many records follow it; and, on the order's own partition, its records from
   * there on (Kafka keeps one key on one partition, in order).
   */
  public ObjectNode kafkaFrom(UUID order, Instant at) throws Exception {
    var spans = kafka.spans(SkuSalesProjection.TOPIC, at);
    int partitions = spans.size();
    int partition = KafkaHistory.partitionFor(order.toString(), partitions);
    var own =
        spans.stream()
            .filter(s -> s.partition() == partition)
            .map(s -> new KafkaHistory.Span(s.partition(), s.from(), s.end()))
            .toList();
    // Bounded: a replay from long ago would otherwise read the whole partition.
    var records =
        kafka.read(SkuSalesProjection.TOPIC, own, r -> order.toString().equals(r.key()), 100);
    var result = json.createObjectNode();
    result.put("topic", SkuSalesProjection.TOPIC);
    result.put("from", at == null ? null : at.toString());
    result.put("partition", partition);
    result.set("spans", json.valueToTree(spans));
    result.put("recordsSince", spans.stream().mapToLong(KafkaHistory.Span::records).sum());
    var list = new ArrayList<ObjectNode>();
    for (var r : records) {
      var node = json.createObjectNode();
      node.put("partition", r.partition());
      node.put("offset", r.offset());
      node.put("timestamp", r.timestamp().toString());
      node.set("headers", json.valueToTree(r.headers()));
      var value = r.value() == null ? null : json.readTree(r.value());
      node.put("eventId", value == null ? null : value.path("eventId").asString(null));
      node.put("type", value == null ? null : value.path("type").asString(null));
      node.put("schemaVersion", value == null ? 0 : value.path("schemaVersion").asInt(1));
      list.add(node);
    }
    result.set("records", json.valueToTree(list));
    return result;
  }

  /**
   * The first record for {@code order} of {@code type} on {@code topic}, searching its partition
   * from {@code since} (by Kafka's time index); null if there is none.
   */
  public KafkaHistory.Record find(String topic, UUID order, String type, Instant since)
      throws Exception {
    var spans = kafka.spans(topic, since);
    int partition = KafkaHistory.partitionFor(order.toString(), spans.size());
    var own = spans.stream().filter(s -> s.partition() == partition).toList();
    var found =
        kafka.read(
            topic,
            own,
            r ->
                order.toString().equals(r.key())
                    && (type == null || type.equals(typeOf(r.value()))),
            1);
    return found.isEmpty() ? null : found.getFirst();
  }

  /**
   * The envelope's type. Parsed, not searched for: Debezium relays the outbox's JSONB column, whose
   * text form is PostgreSQL's ({@code "type": "OrderPlaced"}, with a space), not the writer's.
   */
  private String typeOf(String value) {
    if (value == null) return null;
    try {
      return json.readTree(value).path("type").asString(null);
    } catch (RuntimeException notJson) {
      return null;
    }
  }

  static List<JsonNode> list(JsonNode array) {
    var result = new ArrayList<JsonNode>();
    if (array != null && array.isArray()) array.forEach(result::add);
    return result;
  }
}
