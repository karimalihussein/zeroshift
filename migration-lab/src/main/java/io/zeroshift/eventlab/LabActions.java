package io.zeroshift.eventlab;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Every lever the dashboard offers. Each one acts on the real system (a service's admin API, Kafka
 * Connect's REST API or a Kafka topic) and is recorded with its outcome in operator_action.
 */
@Component
public class LabActions implements AutoCloseable {
  private final LabServices services;
  private final EventLabSettings settings;
  private final JdbcTemplate jdbc;
  private final JsonMapper json = JsonMapper.builder().build();
  private KafkaProducer<String, String> producer;

  public LabActions(LabServices services, EventLabSettings settings, JdbcTemplate jdbc) {
    this.services = services;
    this.settings = settings;
    this.jdbc = jdbc;
  }

  public JsonNode run(String action, JsonNode p) {
    var target =
        p.path("service").asString(p.path("connector").asString(p.path("topic").asString("lab")));
    try {
      var result = perform(action, p);
      log(action, target, summary(result), true);
      return result;
    } catch (RuntimeException e) {
      var message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
      log(action, target, message, false);
      throw new LabServices.ActionFailed(message);
    }
  }

  private JsonNode perform(String action, JsonNode p) {
    String service = p.path("service").asString("");
    return switch (action) {
      case "place-order" -> services.post("order-service", "/orders", p.get("order"));
      case "dual-write" ->
          services.post(
              "order-service",
              "/orders/dual-write?mode=" + p.path("mode").asString(),
              p.get("order"));
      case "consumer-pause", "consumer-resume" ->
          services.post(
              service,
              "/lab/consumers/"
                  + p.path("consumer").asString()
                  + "/"
                  + action.substring("consumer-".length()),
              null);
      case "consumer-seek" ->
          services.post(
              service,
              "/lab/consumers/"
                  + p.path("consumer").asString()
                  + "/seek?topic="
                  + p.path("topic").asString()
                  + "&partition="
                  + p.path("partition").asInt()
                  + "&offset="
                  + p.path("offset").asLong(),
              null);
      case "crash" -> services.post(service, "/lab/crash", null);
      case "fault-arm" ->
          services.send(
              url(service)
                  + "/lab/faults/"
                  + p.path("name").asString()
                  + "?mode="
                  + p.path("mode").asString()
                  + (p.hasNonNull("times") ? "&times=" + p.path("times").asInt() : ""),
              "PUT",
              null);
      case "fault-clear" ->
          services.send(url(service) + "/lab/faults/" + p.path("name").asString(), "DELETE", null);
      case "gateway" ->
          services.post("payment-service", "/lab/gateway/" + p.path("mode").asString(), null);
      case "breaker-reset" -> services.post("payment-service", "/lab/gateway/breaker/reset", null);
      case "connector-pause", "connector-resume" ->
          services.send(
              settings.connectUrl()
                  + "/connectors/"
                  + p.path("connector").asString()
                  + "/"
                  + action.substring("connector-".length()),
              "PUT",
              null);
      case "rebuild-projection" ->
          services.post("order-query-service", "/lab/projection/rebuild", null);
      case "discard-snapshot" ->
          services.send(
              url("order-service") + "/orders/" + p.path("orderId").asString() + "/snapshot",
              "DELETE",
              null);
      case "duplicate" ->
          duplicate(
              p.path("topic").asString(), p.path("partition").asInt(), p.path("offset").asLong());
      case "redrive" ->
          redrive(
              p.path("topic").asString(), p.path("partition").asInt(), p.path("offset").asLong());
      case "poison" ->
          produce(
              p.path("topic").asString(),
              "poison-" + UUID.randomUUID(),
              "{\"type\":\"OrderTeleported\",\"note\":\"no consumer understands this\"}",
              Map.of());
      case "retention-demo" -> {
        for (int i = 0; i < 20; i++)
          produce(
              "lab.retention-demo",
              "demo",
              "{\"n\":" + i + ",\"at\":\"" + java.time.Instant.now() + "\"}",
              Map.of());
        yield json.createObjectNode().put("produced", 20);
      }
      default -> throw new LabServices.ActionFailed("Unknown action " + action);
    };
  }

  /** Publishes an already-published record again: same key, value and headers, new offset. */
  private JsonNode duplicate(String topic, int partition, long offset) {
    var row = tapped(topic, partition, offset);
    return produce(topic, (String) row.get("record_key"), (String) row.get("value"), headers(row));
  }

  /** Sends a dead letter back to the topic it came from, for another attempt. */
  private JsonNode redrive(String dlt, int partition, long offset) {
    var row = tapped(dlt, partition, offset);
    var headers = headers(row);
    var original =
        headers.getOrDefault("kafka_dlt-original-topic", dlt.replaceFirst("\\.dlt$", ""));
    headers.keySet().removeIf(k -> k.startsWith("kafka_dlt"));
    headers.put("zeroshift-redriven-from", dlt + "-" + partition + "@" + offset);
    return produce(original, (String) row.get("record_key"), (String) row.get("value"), headers);
  }

  private Map<String, Object> tapped(String topic, int partition, long offset) {
    return jdbc
        .queryForList(
            "SELECT record_key,value,headers::text AS headers FROM event_tap WHERE topic=? AND kafka_partition=? AND kafka_offset=?",
            topic,
            partition,
            offset)
        .stream()
        .findFirst()
        .orElseThrow(
            () ->
                new LabServices.ActionFailed(
                    "No tapped record " + topic + "-" + partition + "@" + offset));
  }

  private Map<String, String> headers(Map<String, Object> row) {
    var result = new LinkedHashMap<String, String>();
    json.readTree((String) row.get("headers"))
        .properties()
        .forEach(
            e -> result.put(e.getKey(), e.getValue().isNull() ? null : e.getValue().asString()));
    return result;
  }

  private JsonNode produce(String topic, String key, String value, Map<String, String> headers) {
    var record = new ProducerRecord<>(topic, key, value);
    headers.forEach(
        (k, v) -> {
          if (v != null)
            record.headers().add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8)));
        });
    try {
      var meta = producer().send(record).get(5, TimeUnit.SECONDS);
      return json.createObjectNode()
          .put("topic", meta.topic())
          .put("partition", meta.partition())
          .put("offset", meta.offset());
    } catch (Exception e) {
      throw new LabServices.ActionFailed("Publish to " + topic + " failed: " + e.getMessage());
    }
  }

  private synchronized KafkaProducer<String, String> producer() {
    if (producer == null) {
      var props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.kafkaBootstrap());
      props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 4000);
      producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }
    return producer;
  }

  private String url(String service) {
    return Objects.requireNonNull(settings.services().get(service), "Unknown service " + service);
  }

  private void log(String action, String target, String outcome, boolean ok) {
    jdbc.update(
        "INSERT INTO operator_action(action,target,outcome,ok) VALUES(?,?,?,?)",
        action,
        target,
        outcome.substring(0, Math.min(outcome.length(), 500)),
        ok);
  }

  private static String summary(JsonNode result) {
    var text = result == null ? "ok" : result.toString();
    return text.length() > 300 ? text.substring(0, 300) + "…" : text;
  }

  @Override
  public synchronized void close() {
    if (producer != null) producer.close();
  }
}
