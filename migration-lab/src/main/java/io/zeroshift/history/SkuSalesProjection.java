package io.zeroshift.history;

import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.contracts.OrderEvent;
import io.zeroshift.contracts.OrderLine;
import io.zeroshift.contracts.Topics;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.core.type.TypeReference;

/**
 * Sales per SKU, built by the control plane from order.events: a new projection over old history.
 * Two versions of its logic exist on purpose.
 *
 * <ul>
 *   <li><b>v1</b> (wrong): counts an order's lines when the order is <em>placed</em>. Orders that
 *       are later cancelled (payment declined, out of stock) stay counted as sales.
 *   <li><b>v2</b> (fixed): holds the lines when the order is placed, counts them when it
 *       <em>ships</em>, forgets them when it is cancelled.
 * </ul>
 *
 * Each version keeps its own rows and its own Kafka position, stored in the same transaction as the
 * rows: after a crash it is exactly where its offsets say. Rebuilding is deleting both and reading
 * order.events again from the start (or from a point in time).
 */
@Component
public class SkuSalesProjection {
  public static final String V1 = "sku-sales-v1";
  public static final String V2 = "sku-sales-v2";
  static final String TOPIC = Topics.ORDER_EVENTS;

  public record Sales(String sku, long units, BigDecimal revenue, long orders) {}

  public record Skipped(int partition, long offset, String reason) {}

  /**
   * @param from the offsets the replay started at, per partition
   * @param to the offsets it reached (the log end when it began)
   */
  public record Replay(
      String projection,
      Instant startedAt,
      long millis,
      Map<Integer, Long> from,
      Map<Integer, Long> to,
      long records,
      long applied,
      long skipped) {}

  private final KafkaHistory kafka;
  private final JdbcTemplate jdbc;
  private final TransactionTemplate transactions;

  public SkuSalesProjection(
      KafkaHistory kafka, JdbcTemplate jdbc, TransactionTemplate transactions) {
    this.kafka = kafka;
    this.jdbc = jdbc;
    this.transactions = transactions;
  }

  /**
   * Deletes everything {@code projection} built and its position, then replays order.events from
   * {@code since} (null: the first offset still in the log) up to the current end.
   */
  public synchronized Replay rebuild(String projection, Instant since) throws Exception {
    known(projection);
    var spans = kafka.spans(TOPIC, since);
    transactions.executeWithoutResult(
        tx -> {
          for (var table :
              List.of(
                  "history_sku_sales",
                  "history_pending_order",
                  "history_projection_skip",
                  "history_projection_offset"))
            jdbc.update("DELETE FROM " + table + " WHERE projection=?", projection);
          for (var s : spans)
            jdbc.update(
                "INSERT INTO history_projection_offset(projection, kafka_partition, next_offset) VALUES(?,?,?)",
                projection,
                s.partition(),
                s.from());
        });
    return catchUp(projection);
  }

  /** Applies everything written since the projection's stored offsets, in one pass. */
  public synchronized Replay catchUp(String projection) throws Exception {
    known(projection);
    var started = Instant.now();
    long t0 = System.nanoTime();
    var stored = offsets(projection);
    var end = kafka.spans(TOPIC, null);
    var spans = new ArrayList<KafkaHistory.Span>();
    for (var s : end)
      spans.add(
          new KafkaHistory.Span(
              s.partition(), stored.getOrDefault(s.partition(), s.from()), s.end()));
    long[] totals = new long[3]; // records, applied, skipped
    // Each poll's records and the offsets after them commit together.
    kafka.stream(
        TOPIC,
        spans,
        batch -> {
          var counts =
              transactions.execute(
                  tx -> {
                    long a = 0, k = 0;
                    var next = new LinkedHashMap<Integer, Long>();
                    for (var r : batch) {
                      if (apply(projection, r)) a++;
                      else k++;
                      next.put(r.partition(), r.offset() + 1);
                    }
                    next.forEach(
                        (p, o) ->
                            jdbc.update(
                                "INSERT INTO history_projection_offset(projection, kafka_partition, next_offset) VALUES(?,?,?)"
                                    + " ON CONFLICT(projection, kafka_partition) DO UPDATE SET next_offset=EXCLUDED.next_offset",
                                projection,
                                p,
                                o));
                    return new long[] {a, k};
                  });
          totals[0] += batch.size();
          totals[1] += counts[0];
          totals[2] += counts[1];
          return true;
        });
    var reached = new LinkedHashMap<Integer, Long>();
    for (var s : spans) reached.put(s.partition(), s.end());
    var from = new LinkedHashMap<Integer, Long>();
    for (var s : spans) from.put(s.partition(), s.from());
    return new Replay(
        projection,
        started,
        (System.nanoTime() - t0) / 1_000_000,
        from,
        reached,
        totals[0],
        totals[1],
        totals[2]);
  }

  /** Applies one record; false when it was skipped (unreadable) rather than applied. */
  private boolean apply(String projection, KafkaHistory.Record r) {
    OrderEvent event;
    try {
      // The deployed reader: upcasts older versions, refuses newer ones and anything malformed.
      if (!(MessageCodec.decode(r.value()).payload() instanceof OrderEvent e)) return true;
      event = e;
    } catch (RuntimeException e) {
      jdbc.update(
          "INSERT INTO history_projection_skip(projection, kafka_partition, kafka_offset, reason) VALUES(?,?,?,?)"
              + " ON CONFLICT DO NOTHING",
          projection,
          r.partition(),
          r.offset(),
          e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
      return false;
    }
    if (projection.equals(V1)) {
      if (event instanceof OrderEvent.OrderPlaced p) count(projection, p.lines());
      return true;
    }
    switch (event) {
      case OrderEvent.OrderPlaced p ->
          jdbc.update(
              "INSERT INTO history_pending_order(projection, order_id, lines) VALUES(?,?,?::jsonb) ON CONFLICT DO NOTHING",
              projection,
              p.orderId(),
              MessageCodec.json().writeValueAsString(p.lines()));
      case OrderEvent.OrderShipped s -> {
        var lines =
            jdbc.query(
                "DELETE FROM history_pending_order WHERE projection=? AND order_id=? RETURNING lines::text",
                (rs, i) -> rs.getString(1),
                projection,
                s.orderId());
        for (var json : lines)
          count(
              projection,
              MessageCodec.json().readValue(json, new TypeReference<List<OrderLine>>() {}));
      }
      case OrderEvent.OrderCancelled c ->
          jdbc.update(
              "DELETE FROM history_pending_order WHERE projection=? AND order_id=?",
              projection,
              c.orderId());
      default -> {}
    }
    return true;
  }

  private void count(String projection, List<OrderLine> lines) {
    for (var line : lines)
      jdbc.update(
          "INSERT INTO history_sku_sales(projection, sku, units, revenue, orders) VALUES(?,?,?,?,1)"
              + " ON CONFLICT(projection, sku) DO UPDATE SET units=history_sku_sales.units+EXCLUDED.units,"
              + " revenue=history_sku_sales.revenue+EXCLUDED.revenue, orders=history_sku_sales.orders+1",
          projection,
          line.sku(),
          line.quantity(),
          line.subtotal());
  }

  public List<Sales> sales(String projection) {
    return jdbc.query(
        "SELECT sku, units, revenue, orders FROM history_sku_sales WHERE projection=? ORDER BY sku",
        (rs, i) -> new Sales(rs.getString(1), rs.getLong(2), rs.getBigDecimal(3), rs.getLong(4)),
        projection);
  }

  public List<Skipped> skipped(String projection) {
    return jdbc.query(
        "SELECT kafka_partition, kafka_offset, reason FROM history_projection_skip WHERE projection=? ORDER BY kafka_partition, kafka_offset",
        (rs, i) -> new Skipped(rs.getInt(1), rs.getLong(2), rs.getString(3)),
        projection);
  }

  public long pending(String projection) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM history_pending_order WHERE projection=?", Long.class, projection);
  }

  public Map<Integer, Long> offsets(String projection) {
    var result = new LinkedHashMap<Integer, Long>();
    jdbc.query(
        "SELECT kafka_partition, next_offset FROM history_projection_offset WHERE projection=? ORDER BY kafka_partition",
        rs -> {
          result.put(rs.getInt(1), rs.getLong(2));
        },
        projection);
    return result;
  }

  private static void known(String projection) {
    if (!projection.equals(V1) && !projection.equals(V2))
      throw new IllegalArgumentException("Projections are " + V1 + " and " + V2);
  }
}
