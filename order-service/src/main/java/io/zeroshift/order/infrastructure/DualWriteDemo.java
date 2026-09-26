package io.zeroshift.order.infrastructure;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.contracts.Topics;
import io.zeroshift.order.application.EventStore;
import io.zeroshift.order.application.OrderTables;
import io.zeroshift.order.application.PlaceOrder;
import io.zeroshift.order.domain.Order;
import io.zeroshift.platform.Crash;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionOperations;

/**
 * The anti-pattern the outbox replaces, kept runnable so its failure can be watched: the database
 * and Kafka are written separately, and no transaction spans both. It starts no saga; it exists
 * only to show an event lost or invented.
 */
public class DualWriteDemo {
  public enum Mode {
    /** Commit the order, then die before publishing: the order exists, its event never will. */
    COMMIT_THEN_CRASH,
    /** Publish, then fail the transaction: consumers see an order that does not exist. */
    PUBLISH_THEN_ROLLBACK
  }

  public record Result(UUID orderId, UUID eventId, String outcome) {}

  private final PlaceOrder placeOrder;
  private final EventStore events;
  private final OrderTables tables;
  private final KafkaTemplate<String, String> kafka;
  private final TransactionOperations transactions;

  public DualWriteDemo(
      PlaceOrder placeOrder,
      EventStore events,
      OrderTables tables,
      KafkaTemplate<String, String> kafka,
      TransactionOperations transactions) {
    this.placeOrder = placeOrder;
    this.events = events;
    this.tables = tables;
    this.kafka = kafka;
    this.transactions = transactions;
  }

  public Result place(UUID customerId, List<PlaceOrder.Item> items, String voucherCode, Mode mode) {
    var id = UUID.randomUUID();
    var quote = placeOrder.quote(customerId, items, voucherCode);
    if (mode == Mode.PUBLISH_THEN_ROLLBACK) {
      var published = new Envelope[1];
      try {
        transactions.executeWithoutResult(
            tx -> {
              published[0] = write(id, quote);
              publish(published[0]);
              throw new IllegalStateException("database write failed after the Kafka publish");
            });
      } catch (IllegalStateException expected) {
        return new Result(
            id,
            published[0] == null ? null : published[0].eventId(),
            "OrderPlaced is on Kafka, but the order was rolled back: consumers saw a ghost order");
      }
    }
    var placed = transactions.execute(tx -> write(id, quote));
    Crash.now("order " + id + " committed; dying before its Kafka publish, so the event is lost");
    return new Result(id, placed.eventId(), "unreachable");
  }

  /**
   * The database half of the dual write: everything the normal path writes (the event, the order,
   * its invoice, the voucher's use) except the outbox row and the saga.
   */
  private Envelope write(UUID id, PlaceOrder.Quote quote) {
    var event = placeOrder.placed(id, quote);
    var placed = Envelope.of(event, UUID.randomUUID(), null);
    events.append(id, 0, List.of(placed));
    tables.recorded(Order.empty(id).apply(event), List.of(event));
    return placed;
  }

  private void publish(Envelope envelope) {
    try {
      kafka
          .send(Topics.ORDER_EVENTS, envelope.orderId().toString(), MessageCodec.encode(envelope))
          .get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new IllegalStateException("Kafka publish failed: " + e.getMessage(), e);
    }
  }
}
