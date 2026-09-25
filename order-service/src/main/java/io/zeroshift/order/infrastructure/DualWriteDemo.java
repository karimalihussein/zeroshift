package io.zeroshift.order.infrastructure;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.contracts.Topics;
import io.zeroshift.order.application.EventStore;
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

  private final PlaceOrder pricing;
  private final EventStore events;
  private final KafkaTemplate<String, String> kafka;
  private final TransactionOperations transactions;

  public DualWriteDemo(
      PlaceOrder pricing,
      EventStore events,
      KafkaTemplate<String, String> kafka,
      TransactionOperations transactions) {
    this.pricing = pricing;
    this.events = events;
    this.kafka = kafka;
    this.transactions = transactions;
  }

  public Result place(String customerId, List<PlaceOrder.Item> items, Mode mode) {
    var id = UUID.randomUUID();
    var placed =
        Envelope.of(Order.place(id, customerId, pricing.price(items)), UUID.randomUUID(), null);
    if (mode == Mode.PUBLISH_THEN_ROLLBACK) {
      try {
        transactions.executeWithoutResult(
            tx -> {
              events.append(id, 0, List.of(placed));
              publish(placed);
              throw new IllegalStateException("database write failed after the Kafka publish");
            });
      } catch (IllegalStateException expected) {
        return new Result(
            id,
            placed.eventId(),
            "OrderPlaced is on Kafka, but the order was rolled back: consumers saw a ghost order");
      }
    }
    transactions.executeWithoutResult(tx -> events.append(id, 0, List.of(placed)));
    Crash.now("order " + id + " committed; dying before its Kafka publish, so the event is lost");
    return new Result(id, placed.eventId(), "unreachable");
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
