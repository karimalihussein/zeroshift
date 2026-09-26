package io.zeroshift.order.infrastructure;

import static io.zeroshift.order.db.Tables.EVENT_STORE;

import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.contracts.OrderEvent;
import io.zeroshift.contracts.OrderLine;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.jooq.DSLContext;

/**
 * What was really sold, per SKU, from the source of truth: every shipped order's lines, read from
 * the event store (not from Kafka, not from any projection). The history lab compares projections
 * built from order.events against this.
 */
public final class ShippedSales {
  public record Sales(String sku, long units, BigDecimal revenue, long orders) {}

  private final DSLContext db;

  public ShippedSales(DSLContext db) {
    this.db = db;
  }

  public List<Sales> bySku() {
    var lines = new HashMap<UUID, List<OrderLine>>();
    var shipped = new java.util.HashSet<UUID>();
    db.select(EVENT_STORE.ENVELOPE)
        .from(EVENT_STORE)
        .where(EVENT_STORE.TYPE.in("OrderPlaced", "OrderShipped"))
        .fetchLazy()
        .forEach(
            r -> {
              switch (MessageCodec.decode(r.value1().data()).payload()) {
                case OrderEvent.OrderPlaced p -> lines.put(p.orderId(), p.lines());
                case OrderEvent.OrderShipped s -> shipped.add(s.orderId());
                default -> {}
              }
            });
    var sums = new TreeMap<String, long[]>();
    var revenue = new TreeMap<String, BigDecimal>();
    for (var id : shipped)
      for (var line : lines.getOrDefault(id, List.of())) {
        var s = sums.computeIfAbsent(line.sku(), k -> new long[2]);
        s[0] += line.quantity();
        s[1]++;
        revenue.merge(line.sku(), line.subtotal(), BigDecimal::add);
      }
    return sums.entrySet().stream()
        .map(e -> new Sales(e.getKey(), e.getValue()[0], revenue.get(e.getKey()), e.getValue()[1]))
        .toList();
  }

  /** How many stored events of each type exist at each schema version. */
  public Map<String, Map<Integer, Integer>> versions() {
    var result = new TreeMap<String, Map<Integer, Integer>>();
    db.select(EVENT_STORE.TYPE, EVENT_STORE.SCHEMA_VERSION, org.jooq.impl.DSL.count())
        .from(EVENT_STORE)
        .groupBy(EVENT_STORE.TYPE, EVENT_STORE.SCHEMA_VERSION)
        .forEach(
            r ->
                result
                    .computeIfAbsent(r.value1(), k -> new TreeMap<>())
                    .put(r.value2(), r.value3()));
    return result;
  }
}
