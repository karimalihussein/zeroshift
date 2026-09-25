package io.zeroshift.order.application;

import io.zeroshift.contracts.OrderLine;
import io.zeroshift.contracts.PaymentCommand.AuthorizePayment;
import io.zeroshift.order.domain.Order;
import io.zeroshift.order.domain.OrderRuleViolation;
import io.zeroshift.order.domain.Saga;
import io.zeroshift.platform.Outbox;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.springframework.transaction.support.TransactionOperations;

/**
 * One transaction: the OrderPlaced event, its outbox row, the saga and the AuthorizePayment
 * command. Either all of it happens or none of it; there is no second write to Kafka that could
 * fail alone.
 *
 * <p>With an Idempotency-Key, the key is claimed in that same transaction. A client that timed out
 * and retries gets the first request's answer instead of a second order: the retry finds the key,
 * or, if both run at once, waits on it and then finds it.
 */
public final class PlaceOrder {
  public record Item(String sku, int quantity) {}

  /** {@code version} is the order's version after this write: a read-your-writes token. */
  public record Placed(
      UUID orderId,
      UUID correlationId,
      UUID eventId,
      BigDecimal total,
      long version,
      boolean replayed) {}

  /** The same key was sent with a different order: never answer it with the first one's result. */
  public static final class IdempotencyConflict extends RuntimeException {
    public IdempotencyConflict(String message) {
      super(message);
    }
  }

  /** Thrown inside the transaction to roll it back when a concurrent request won the key. */
  private static final class KeyTaken extends RuntimeException {
    KeyTaken() {
      super(null, null, false, false);
    }
  }

  private final Catalog catalog;
  private final IdempotencyKeys keys;
  private final OrderRepository orders;
  private final SagaStore sagas;
  private final Outbox outbox;
  private final OrderSaga saga;
  private final TransactionOperations transactions;
  private final Clock clock;

  public PlaceOrder(
      Catalog catalog,
      IdempotencyKeys keys,
      OrderRepository orders,
      SagaStore sagas,
      Outbox outbox,
      OrderSaga saga,
      TransactionOperations transactions,
      Clock clock) {
    this.catalog = catalog;
    this.keys = keys;
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
    return place(customerId, items, null);
  }

  public Placed place(String customerId, List<Item> items, String idempotencyKey) {
    var lines = price(items);
    if (idempotencyKey == null) return transactions.execute(tx -> create(customerId, lines, null));
    if (idempotencyKey.isBlank() || idempotencyKey.length() > 200)
      throw new OrderRuleViolation("Idempotency-Key must be 1 to 200 characters");
    var hash = requestHash(customerId, items);
    var answered = keys.find(idempotencyKey);
    if (answered.isPresent()) return replay(answered.get(), hash);
    try {
      return transactions.execute(
          tx ->
              create(
                  customerId,
                  lines,
                  new IdempotencyKeys.Claim(idempotencyKey, hash, null, null, null, null)));
    } catch (KeyTaken concurrentWinner) {
      return replay(keys.find(idempotencyKey).orElseThrow(), hash);
    }
  }

  private Placed create(String customerId, List<OrderLine> lines, IdempotencyKeys.Claim key) {
    var id = UUID.randomUUID();
    var correlationId = UUID.randomUUID();
    var placed = Order.place(id, customerId, lines);
    var appended = orders.record(Order.empty(id), null, correlationId, placed);
    var order = appended.order();
    var placedEvent = appended.events().getFirst();
    var now = clock.instant();
    sagas.start(Saga.start(id, correlationId, now, now.plus(saga.stepTimeout())));
    outbox.append(placedEvent.reply(new AuthorizePayment(id, order.total(), order.currency())));
    if (key != null
        && !keys.claim(
            new IdempotencyKeys.Claim(
                key.key(),
                key.requestHash(),
                id,
                correlationId,
                placedEvent.eventId(),
                order.total())))
      throw new KeyTaken(); // rolls back this order: the other request's order is the answer
    return new Placed(
        id, correlationId, placedEvent.eventId(), order.total(), order.version(), false);
  }

  private static Placed replay(IdempotencyKeys.Claim claim, String hash) {
    if (!claim.requestHash().equals(hash))
      throw new IdempotencyConflict(
          "Idempotency-Key " + claim.key() + " was already used for a different order");
    return new Placed(
        claim.orderId(), claim.correlationId(), claim.eventId(), claim.total(), 1, true);
  }

  /** The request, reduced to what makes two requests "the same order". */
  static String requestHash(String customerId, List<Item> items) {
    var canonical =
        customerId
            + "|"
            + String.join(
                ",",
                items.stream()
                    .sorted(Comparator.comparing(Item::sku))
                    .map(i -> i.sku() + ":" + i.quantity())
                    .toList());
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256")
                  .digest(canonical.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
