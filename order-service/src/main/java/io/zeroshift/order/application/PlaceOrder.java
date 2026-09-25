package io.zeroshift.order.application;

import io.zeroshift.contracts.OrderLine;
import io.zeroshift.contracts.PaymentCommand.AuthorizePayment;
import io.zeroshift.order.domain.Order;
import io.zeroshift.order.domain.OrderRuleViolation;
import io.zeroshift.order.domain.Saga;
import io.zeroshift.platform.Outbox;
import java.math.BigDecimal;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import org.springframework.transaction.support.TransactionOperations;

/**
 * One transaction: the OrderPlaced event, its outbox row, the saga and the AuthorizePayment
 * command. Either all of it happens or none of it; there is no second write to Kafka that could
 * fail alone.
 */
public final class PlaceOrder {
  public record Item(String sku, int quantity) {}

  public record Placed(UUID orderId, UUID correlationId, UUID eventId, BigDecimal total) {}

  private final Catalog catalog;
  private final OrderRepository orders;
  private final SagaStore sagas;
  private final Outbox outbox;
  private final OrderSaga saga;
  private final TransactionOperations transactions;
  private final Clock clock;

  public PlaceOrder(
      Catalog catalog,
      OrderRepository orders,
      SagaStore sagas,
      Outbox outbox,
      OrderSaga saga,
      TransactionOperations transactions,
      Clock clock) {
    this.catalog = catalog;
    this.orders = orders;
    this.sagas = sagas;
    this.outbox = outbox;
    this.saga = saga;
    this.transactions = transactions;
    this.clock = clock;
  }

  public List<OrderLine> price(List<Item> items) {
    if (items == null || items.isEmpty()) throw new OrderRuleViolation("An order needs a line");
    return items.stream()
        .map(
            item ->
                new OrderLine(
                    item.sku(),
                    item.quantity(),
                    catalog
                        .find(item.sku())
                        .orElseThrow(() -> new OrderRuleViolation("Unknown SKU " + item.sku()))
                        .price()))
        .toList();
  }

  public Placed place(String customerId, List<Item> items) {
    var lines = price(items);
    return transactions.execute(
        tx -> {
          var id = UUID.randomUUID();
          var correlationId = UUID.randomUUID();
          var placed = Order.place(id, customerId, lines);
          var appended = orders.record(Order.empty(id), null, correlationId, placed);
          var order = appended.order();
          var placedEvent = appended.events().getFirst();
          var now = clock.instant();
          sagas.start(Saga.start(id, correlationId, now, now.plus(saga.stepTimeout())));
          outbox.append(
              placedEvent.reply(new AuthorizePayment(id, order.total(), order.currency())));
          return new Placed(id, correlationId, placedEvent.eventId(), order.total());
        });
  }
}
