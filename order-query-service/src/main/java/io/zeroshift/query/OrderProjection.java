package io.zeroshift.query;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.OrderEvent;
import io.zeroshift.contracts.OrderEvent.*;
import io.zeroshift.platform.Handled;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Folds order.events into the read models: decides what each event changes, {@link ReadModels}
 * writes it. It trusts only what the topic says: an event for an order it never saw placed is
 * ignored, and a ghost order (published but never committed, see the dual-write demo) is projected
 * like any other, because the read side cannot tell.
 *
 * <p>Correctness rests on two things outside this class: the inbox applies each event at most once
 * (so counters are not double-counted), and Kafka keeps one order's events in order on one
 * partition (so a later status never lands before an earlier one).
 */
public final class OrderProjection {
  private final ReadModels readModels;

  public OrderProjection(ReadModels readModels) {
    this.readModels = readModels;
  }

  public Handled apply(Envelope envelope, ConsumerRecord<?, ?> record) {
    if (!(envelope.payload() instanceof OrderEvent event))
      return Handled.ignored("Not an order event: " + envelope.type());
    var at =
        new ReadModels.Position(
            envelope.eventId(), envelope.type(), record.partition() + "@" + record.offset());
    if (event instanceof OrderPlaced p)
      return readModels.placed(p, at, envelope.occurredAt())
          ? Handled.processed("order_view row created; customer_summary +1 placed")
          : Handled.ignored("order_view already has " + p.orderId());
    boolean applied =
        switch (event) {
          case OrderPaymentAuthorized e -> readModels.paid(e.orderId(), e.paymentId(), at);
          case OrderStockReserved e -> readModels.reserved(e.orderId(), e.reservationId(), at);
          case OrderShipped e -> readModels.shipped(e.orderId(), e.trackingNumber(), at);
          case OrderCancelled e ->
              readModels.cancelled(e.orderId(), e.reason(), e.compensations(), at);
          case OrderPlaced e -> throw new IllegalStateException("handled above");
        };
    return applied
        ? Handled.processed("order_view → " + envelope.type())
        : Handled.ignored("No order_view row for " + event.orderId());
  }
}
