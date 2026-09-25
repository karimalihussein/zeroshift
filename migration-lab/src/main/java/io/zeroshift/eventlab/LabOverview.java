package io.zeroshift.eventlab;

import java.util.*;
import java.util.concurrent.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/** One poll's worth of lab state, fetched in parallel from every source it comes from. */
@Component
public class LabOverview {
  private final LabServices services;
  private final KafkaInspector kafka;
  private final EventTap tap;
  private final JdbcTemplate jdbc;
  private final JsonMapper json = JsonMapper.builder().build();
  private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

  public LabOverview(LabServices services, KafkaInspector kafka, EventTap tap, JdbcTemplate jdbc) {
    this.services = services;
    this.kafka = kafka;
    this.tap = tap;
    this.jdbc = jdbc;
  }

  /**
   * Outbox rows (of the newest 200) whose event id the tap has not seen on Kafka: committed, but
   * not yet relayed by Debezium. Exact, unlike WAL bytes, which include other databases' traffic.
   */
  private ObjectNode unpublished(JsonNode rows) {
    var ids = new ArrayList<UUID>();
    for (var row : rows) ids.add(UUID.fromString(row.path("id").asString()));
    var seen =
        new HashSet<>(
            jdbc.queryForList(
                "SELECT event_id FROM event_tap WHERE event_id = ANY(?)",
                UUID.class,
                (Object) ids.toArray(UUID[]::new)));
    var pending = json.createObjectNode();
    long count = 0;
    String oldest = null;
    for (var row : rows)
      if (!seen.contains(UUID.fromString(row.path("id").asString()))) {
        count++;
        oldest = row.path("createdAt").asString();
      }
    return pending.put("count", count).put("oldest", oldest);
  }

  public ObjectNode snapshot() throws InterruptedException {
    var tasks = new LinkedHashMap<String, Callable<Object>>();
    for (var service : services.names())
      tasks.put("service:" + service, () -> services.get(service, "/lab/state"));
    tasks.put(
        "orders",
        () -> LabServices.data(services.tryGetAnyReplica("order-service", "/orders?limit=25")));
    tasks.put(
        "catalog", () -> LabServices.data(services.tryGetAnyReplica("order-service", "/catalog")));
    tasks.put("gateway", () -> services.get("payment-service", "/lab/gateway"));
    tasks.put("stock", () -> LabServices.data(services.get("inventory-service", "/stock")));
    tasks.put("projection", () -> services.get("order-query-service", "/lab/projection"));
    tasks.put(
        "readModel",
        () -> LabServices.data(services.get("order-query-service", "/orders?limit=25")));
    tasks.put("connectors", () -> services.connect("/connectors?expand=status"));
    tasks.put("topics", kafka::topics);
    tasks.put("groups", kafka::groups);
    // Per database, not per replica: replicas share their service's tables.
    for (var service : services.names()) {
      if (LabServices.isReplica(service)) continue;
      tasks.put(
          "decisions:" + service,
          () -> LabServices.data(services.tryGetAnyReplica(service, "/lab/decisions?limit=40")));
      tasks.put(
          "outbox:" + service,
          () -> LabServices.data(services.tryGetAnyReplica(service, "/lab/outbox?limit=200")));
    }
    tasks.put("lease", () -> services.tryGetAnyReplica("order-service", "/lab/lease"));
    var futures = new LinkedHashMap<String, Future<Object>>();
    tasks.forEach((k, t) -> futures.put(k, pool.submit(t)));

    var result = json.createObjectNode();
    var serviceStates = result.putObject("services");
    var decisions = new ArrayList<JsonNode>();
    var unpublished = result.putObject("unpublished");
    for (var entry : futures.entrySet()) {
      JsonNode value;
      try {
        value = json.valueToTree(entry.getValue().get(6, TimeUnit.SECONDS));
      } catch (ExecutionException | TimeoutException e) {
        value =
            services.error(
                e instanceof ExecutionException ee && ee.getCause() instanceof Exception c ? c : e);
      }
      var key = entry.getKey();
      if (key.startsWith("service:")) serviceStates.set(key.substring(8), value);
      else if (key.startsWith("outbox:")) {
        if (value.isArray() && !value.isEmpty())
          unpublished.set(key.substring(7), unpublished(value));
      } else if (key.startsWith("decisions:")) {
        if (value.isArray())
          for (var d : value) decisions.add(((ObjectNode) d).put("service", key.substring(10)));
      } else result.set(key, value);
    }
    decisions.sort(Comparator.comparing((JsonNode d) -> d.path("at").asString()).reversed());
    var merged = result.putArray("decisions");
    decisions.stream().limit(60).forEach(merged::add);
    result.set(
        "deadLetters",
        json.valueToTree(
            jdbc.queryForList(
                "SELECT topic,kafka_partition,kafka_offset,record_key,type,headers::text AS headers,value,kafka_timestamp"
                    + " FROM event_tap WHERE topic LIKE '%.dlt' ORDER BY kafka_timestamp DESC LIMIT 30")));
    result.set("tap", json.valueToTree(tap.status()));
    result.set(
        "actions",
        json.valueToTree(
            jdbc.queryForList(
                "SELECT action,target,outcome,ok,at FROM operator_action ORDER BY id DESC LIMIT 25")));
    return result;
  }
}
