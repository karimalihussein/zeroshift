package io.zeroshift.history;

import io.zeroshift.eventlab.LabServices;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * A new projection built from old events, first wrong, then fixed and rebuilt from history. The
 * judge is independent of both: shipped orders' lines read from the event store itself.
 */
@Component
public class ProjectionLab implements HistoryLab {
  private final SkuSalesProjection projection;
  private final KafkaHistory kafka;
  private final LabServices services;
  private final OrderHistory orders;
  private final io.zeroshift.eventlab.LabCustomers customers;
  private final JsonMapper json = JsonMapper.builder().build();

  public ProjectionLab(
      SkuSalesProjection projection,
      KafkaHistory kafka,
      LabServices services,
      OrderHistory orders,
      io.zeroshift.eventlab.LabCustomers customers) {
    this.projection = projection;
    this.kafka = kafka;
    this.services = services;
    this.orders = orders;
    this.customers = customers;
  }

  @Override
  public String id() {
    return "projection";
  }

  @Override
  public String title() {
    return "Projection evolution";
  }

  @Override
  public String summary() {
    return "A new read model (sales per SKU) over every order ever placed: built once with a bug,"
        + " fixed, rebuilt from the full history and checked against the event store.";
  }

  @Override
  public List<String> plan() {
    return List.of(
        "Measure the history available: order.events per partition from its first offset, and the event store's events by type and schema version.",
        "Build sales-v1 from offset 0. Its rule counts an order's lines when the order is placed, so every cancelled order is counted as a sale.",
        "Fix the rule (sales-v2 counts lines when the order ships and forgets cancelled ones) and rebuild it from offset 0.",
        "Place three new orders (one over the payment limit, so it is declined and cancelled) and let both projections catch up from their stored offsets only.",
        "Compare v1, v2 and the truth per SKU: shipped orders' lines read from the event store, not from Kafka.",
        "Why a projection is a function of history, and what rebuilding one costs.");
  }

  @Override
  public ObjectNode run(int step, ObjectNode memo) throws Exception {
    var result = json.createObjectNode();
    switch (step) {
      case 0 -> {
        var spans = kafka.spans(SkuSalesProjection.TOPIC, null);
        result.set("partitions", json.valueToTree(spans));
        result.put("records", spans.stream().mapToLong(KafkaHistory.Span::records).sum());
        result.set(
            "eventStore", services.tryGetAnyReplica(OrderHistory.ORDERS, "/lab/history/versions"));
      }
      case 1 -> {
        var replay = projection.rebuild(SkuSalesProjection.V1, null);
        result.set("replay", json.valueToTree(replay));
        result.set("sales", json.valueToTree(projection.sales(SkuSalesProjection.V1)));
        result.set("truth", truth());
        result.set("difference", difference(projection.sales(SkuSalesProjection.V1), truth()));
      }
      case 2 -> {
        var replay = projection.rebuild(SkuSalesProjection.V2, null);
        result.set("replay", json.valueToTree(replay));
        result.set("sales", json.valueToTree(projection.sales(SkuSalesProjection.V2)));
        result.put("pendingOrders", projection.pending(SkuSalesProjection.V2));
        result.set("skipped", json.valueToTree(projection.skipped(SkuSalesProjection.V2)));
      }
      case 3 -> {
        var placed = json.createArrayNode();
        var customer = customers.any(java.util.concurrent.ThreadLocalRandom.current()).id();
        for (var body :
            List.of(
                order(customer, "SKU-CABLE", 2),
                order(customer, "SKU-MOUSE", 1),
                // 12 keyboards (with tax over 1,100), over the payment service's 1000.00 limit.
                order(customer, "SKU-KEYBOARD", 12))) {
          var answer = services.post(OrderHistory.ORDERS, "/orders", body);
          placed.add(answer.path("orderId").asString());
        }
        result.set("placed", placed);
        var states = awaitFinished(placed);
        result.set("finalStates", states);
        result.set("v1", json.valueToTree(projection.catchUp(SkuSalesProjection.V1)));
        result.set("v2", json.valueToTree(projection.catchUp(SkuSalesProjection.V2)));
      }
      case 4 -> {
        // The event store is ahead of Kafka by the relay delay: catch up, compare, and try again
        // for a few seconds if an order shipped in between.
        JsonNode truth = truth();
        for (int attempt = 0; attempt < 8; attempt++) {
          projection.catchUp(SkuSalesProjection.V1);
          projection.catchUp(SkuSalesProjection.V2);
          truth = truth();
          if (difference(projection.sales(SkuSalesProjection.V2), truth).isEmpty()) break;
          Thread.sleep(1500);
        }
        var v1 = projection.sales(SkuSalesProjection.V1);
        var v2 = projection.sales(SkuSalesProjection.V2);
        result.set("truth", truth);
        result.set("v1", json.valueToTree(v1));
        result.set("v2", json.valueToTree(v2));
        result.set("v1MinusTruth", difference(v1, truth));
        result.set("v2MinusTruth", difference(v2, truth));
        result.put("v2MatchesTruth", difference(v2, truth).isEmpty());
        result.put("v1MatchesTruth", difference(v1, truth).isEmpty());
        result.put("v2PendingOrders", projection.pending(SkuSalesProjection.V2));
        result.set("v2Skipped", json.valueToTree(projection.skipped(SkuSalesProjection.V2)));
      }
      case 5 ->
          result.put(
              "text",
              "A projection is a pure function of the event history. Its first version answered"
                  + " \"what was ordered\" when the question was \"what was sold\"; because every"
                  + " event is kept (order.events has no retention limit), fixing it meant changing"
                  + " the rule and replaying from offset 0, with no migration of old rows and no"
                  + " lost information. Both versions store their Kafka offsets in the same"
                  + " transaction as their rows, so a rebuild is: delete rows and offsets, read"
                  + " again. Catching up afterwards reads only what arrived since the stored"
                  + " offsets. The judge is independent: shipped lines from the event store,"
                  + " which never went through Kafka. Records a projection cannot read (an event"
                  + " from a newer writer) are skipped and listed, never silently dropped.");
      default -> throw new IllegalArgumentException("No step " + step);
    }
    return result;
  }

  private JsonNode truth() {
    return services.tryGetAnyReplica(OrderHistory.ORDERS, "/lab/history/shipped-sales");
  }

  /** Per SKU, projection minus truth; only SKUs that differ. */
  private JsonNode difference(List<SkuSalesProjection.Sales> sales, JsonNode truth) {
    var expected = new TreeMap<String, long[]>();
    var revenue = new TreeMap<String, BigDecimal>();
    for (var t : truth) {
      expected.put(
          t.path("sku").asString(),
          new long[] {t.path("units").asLong(), t.path("orders").asLong()});
      revenue.put(t.path("sku").asString(), t.path("revenue").decimalValue());
    }
    var result = json.createArrayNode();
    var seen = new java.util.HashSet<String>();
    for (var s : sales) {
      seen.add(s.sku());
      var e = expected.getOrDefault(s.sku(), new long[2]);
      var r = revenue.getOrDefault(s.sku(), BigDecimal.ZERO);
      if (e[0] != s.units() || r.compareTo(s.revenue()) != 0 || e[1] != s.orders())
        result
            .addObject()
            .put("sku", s.sku())
            .put("units", s.units() - e[0])
            .put("revenue", s.revenue().subtract(r))
            .put("orders", s.orders() - e[1]);
    }
    for (var sku : expected.keySet())
      if (!seen.contains(sku))
        result
            .addObject()
            .put("sku", sku)
            .put("units", -expected.get(sku)[0])
            .put("revenue", revenue.get(sku).negate())
            .put("orders", -expected.get(sku)[1]);
    return result;
  }

  private static Map<String, Object> order(String customer, String sku, int quantity) {
    return Map.of(
        "customerId", customer, "items", List.of(Map.of("sku", sku, "quantity", quantity)));
  }

  /** Waits (up to 90 s) until every order's saga has finished and returns their states. */
  private JsonNode awaitFinished(JsonNode ids) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 90_000;
    var states = json.createObjectNode();
    while (System.currentTimeMillis() < deadline) {
      boolean all = true;
      for (var id : ids) {
        var detail = services.tryGetAnyReplica(OrderHistory.ORDERS, "/orders/" + id.asString());
        var state = detail.path("saga").path("state").asString("");
        states.put(id.asString(), state);
        if (!state.equals("COMPLETED") && !state.equals("CANCELLED")) all = false;
      }
      if (all) {
        // The last events still have to be relayed by Debezium before a catch-up can see them.
        Thread.sleep(3000);
        return states;
      }
      Thread.sleep(1000);
    }
    throw new IllegalStateException(
        "Orders did not finish within 90 s: " + states + " at " + Instant.now());
  }
}
