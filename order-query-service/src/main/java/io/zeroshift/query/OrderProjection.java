package io.zeroshift.query;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.contracts.OrderEvent;
import io.zeroshift.contracts.OrderEvent.*;
import io.zeroshift.contracts.OrderLine;
import io.zeroshift.platform.Handled;
import java.sql.Timestamp;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Folds order.events into the read models. It trusts only what the topic says: an event for an
 * order it never saw placed is ignored, and a ghost order (published but never committed, see the
 * dual-write demo) is projected like any other, because the read side cannot tell.
 */
public final class OrderProjection {
  private final JdbcTemplate jdbc;

  public OrderProjection(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public Handled apply(Envelope envelope, ConsumerRecord<?, ?> record) {
    if (!(envelope.payload() instanceof OrderEvent event))
      return Handled.ignored("Not an order event: " + envelope.type());
    var offset = record.partition() + "@" + record.offset();
    if (event instanceof OrderPlaced p) {
      int inserted =
          jdbc.update(
              "INSERT INTO order_view(order_id,customer_id,status,total,currency,item_count,lines,"
                  + "events_applied,last_event_type,last_event_id,last_offset,placed_at)"
                  + " VALUES(?,?,'PLACED',?,?,?,?::jsonb,1,?,?,?,?) ON CONFLICT DO NOTHING",
              p.orderId(),
              p.customerId(),
              p.total(),
              p.currency(),
              p.lines().stream().mapToInt(OrderLine::quantity).sum(),
              MessageCodec.json().writeValueAsString(p.lines()),
              envelope.type(),
              envelope.eventId(),
              offset,
              Timestamp.from(envelope.occurredAt()));
      if (inserted == 1)
        jdbc.update(
            "INSERT INTO customer_summary(customer_id,orders_placed) VALUES(?,1) ON CONFLICT(customer_id)"
                + " DO UPDATE SET orders_placed=customer_summary.orders_placed+1,updated_at=clock_timestamp()",
            p.customerId());
      return Handled.processed("order_view row created; customer_summary +1 placed");
    }
    int updated =
        switch (event) {
          case OrderPaymentAuthorized e ->
              update(envelope, offset, "status='PAID',payment_id=?", e.paymentId());
          case OrderStockReserved e ->
              update(envelope, offset, "status='RESERVED',reservation_id=?", e.reservationId());
          case OrderShipped e ->
              update(envelope, offset, "status='SHIPPED',tracking_number=?", e.trackingNumber());
          case OrderCancelled e ->
              update(
                  envelope,
                  offset,
                  "status='CANCELLED',cancel_reason=?,compensations=string_to_array(?, '|')",
                  e.reason(),
                  String.join("|", e.compensations()));
          case OrderPlaced e -> throw new IllegalStateException("handled above");
        };
    if (updated == 0) return Handled.ignored("No order_view row for " + event.orderId());
    switch (event) {
      case OrderShipped e ->
          jdbc.update(
              "UPDATE customer_summary c SET orders_shipped=orders_shipped+1,shipped_value=shipped_value+v.total,"
                  + "updated_at=clock_timestamp() FROM order_view v WHERE v.order_id=? AND c.customer_id=v.customer_id",
              e.orderId());
      case OrderCancelled e ->
          jdbc.update(
              "UPDATE customer_summary c SET orders_cancelled=orders_cancelled+1,updated_at=clock_timestamp()"
                  + " FROM order_view v WHERE v.order_id=? AND c.customer_id=v.customer_id",
              e.orderId());
      default -> {}
    }
    return Handled.processed("order_view → " + envelope.type());
  }

  /** Applies one event's column changes to the order's row; 0 if the row does not exist. */
  private int update(Envelope envelope, String offset, String assignments, Object... values) {
    var args = new java.util.ArrayList<Object>(java.util.Arrays.asList(values));
    args.addAll(java.util.List.of(envelope.type(), envelope.eventId(), offset, envelope.orderId()));
    return jdbc.update(
        "UPDATE order_view SET "
            + assignments
            + ",events_applied=events_applied+1,last_event_type=?,"
            + "last_event_id=?,last_offset=?,projected_at=clock_timestamp() WHERE order_id=?",
        args.toArray());
  }
}
