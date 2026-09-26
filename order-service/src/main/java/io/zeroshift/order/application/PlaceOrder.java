package io.zeroshift.order.application;

import io.zeroshift.contracts.OrderEvent.OrderPlaced;
import io.zeroshift.contracts.OrderLine;
import io.zeroshift.contracts.PaymentCommand.AuthorizePayment;
import io.zeroshift.order.domain.Customer;
import io.zeroshift.order.domain.Order;
import io.zeroshift.order.domain.OrderRuleViolation;
import io.zeroshift.order.domain.Pricing;
import io.zeroshift.order.domain.Saga;
import io.zeroshift.order.domain.Voucher;
import io.zeroshift.order.domain.VoucherNotApplicable;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.transaction.support.TransactionOperations;

/**
 * Placing an order happens in two parts. First, outside any transaction, the {@link Quote}: the
 * customer, today's catalog prices (a call to inventory-service, which must not hold a database
 * connection while it waits) and the voucher, priced by {@link Pricing}. Then one transaction: the
 * voucher's use, the invoice number, the OrderPlaced event with its outbox row and its orders,
 * order_item and invoice rows, the saga and the AuthorizePayment command. Either all of it happens
 * or none of it; there is no second write to Kafka that could fail alone.
 *
 * <p>With an Idempotency-Key, the key is claimed in that same transaction. A client that timed out
 * and retries gets the first request's answer instead of a second order: the retry finds the key,
 * or, if both run at once, waits on it and then finds it.
 */
public final class PlaceOrder {
  public record Item(String sku, int quantity) {}

  /** Everything a placement is decided from, gathered before its transaction. */
  public record Quote(Customer customer, Pricing.Priced priced, Voucher voucher) {}

  /** {@code version} is the order's version when answered: a read-your-writes token. */
  public record Placed(
      UUID orderId,
      UUID correlationId,
      UUID eventId,
      String invoiceNumber,
      String currency,
      BigDecimal subtotal,
      BigDecimal discount,
      BigDecimal tax,
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
  private final Customers customers;
  private final Vouchers vouchers;
  private final OrderTables tables;
  private final Pricing pricing;
  private final IdempotencyKeys keys;
  private final OrderRepository orders;
  private final SagaStore sagas;
  private final Outbox outbox;
  private final OrderSaga saga;
  private final TransactionOperations transactions;
  private final Clock clock;

  public PlaceOrder(
      Catalog catalog,
      Customers customers,
      Vouchers vouchers,
      OrderTables tables,
      Pricing pricing,
      IdempotencyKeys keys,
      OrderRepository orders,
      SagaStore sagas,
      Outbox outbox,
      OrderSaga saga,
      TransactionOperations transactions,
      Clock clock) {
    this.catalog = catalog;
    this.customers = customers;
    this.vouchers = vouchers;
    this.tables = tables;
    this.pricing = pricing;
    this.keys = keys;
    this.orders = orders;
    this.sagas = sagas;
    this.outbox = outbox;
    this.saga = saga;
    this.transactions = transactions;
    this.clock = clock;
  }

  /**
   * The customer, the items at today's catalog prices and the voucher's discount. Checks the
   * voucher can be used now; its use is only taken by {@link #placed}, in the order's transaction.
   */
  public Quote quote(UUID customerId, List<Item> items, String voucherCode) {
    if (items == null || items.isEmpty()) throw new OrderRuleViolation("An order needs a line");
    var customer = customers.find(customerId).orElseThrow(() -> new UnknownCustomer(customerId));
    var code = Voucher.normalize(voucherCode);
    Voucher voucher = null;
    if (code != null) {
      voucher =
          vouchers.find(code).orElseThrow(() -> new VoucherNotApplicable("No voucher " + code));
      voucher.requireUsableAt(clock.instant());
      if (voucher.exhausted()) throw new VoucherExhausted(code);
    }
    var products =
        catalog.find(items.stream().map(Item::sku).distinct().toList()).stream()
            .collect(Collectors.toMap(Catalog.Product::sku, Function.identity(), (a, b) -> a));
    var lines =
        items.stream()
            .map(
                item -> {
                  var product = products.get(item.sku());
                  if (product == null) throw new OrderRuleViolation("Unknown SKU " + item.sku());
                  if (!product.active())
                    throw new OrderRuleViolation("SKU " + item.sku() + " is not for sale");
                  if (!pricing.currency().equals(product.currency()))
                    throw new OrderRuleViolation(
                        "SKU " + item.sku() + " is priced in " + product.currency());
                  return OrderLine.of(
                      product.id(),
                      product.sku(),
                      product.name(),
                      item.quantity(),
                      product.price());
                })
            .toList();
    return new Quote(customer, pricing.price(lines, voucher), voucher);
  }

  /**
   * The OrderPlaced event for {@code quote}. Call inside the transaction that records it: it takes
   * the voucher's use and draws the invoice number, which roll back with it.
   */
  public OrderPlaced placed(UUID id, Quote quote) {
    var voucher = quote.voucher();
    if (voucher != null && !vouchers.redeem(voucher.id())) {
      // Usable when quoted, not any more: used up (or switched off) by someone else meanwhile.
      var now = vouchers.find(voucher.code()).orElseThrow();
      now.requireUsableAt(clock.instant());
      throw new VoucherExhausted(voucher.code());
    }
    return Order.place(
        id,
        quote.customer().id().toString(),
        quote.customer().name(),
        quote.priced(),
        voucher == null ? null : voucher.code(),
        tables.nextInvoiceNumber());
  }

  public Placed place(UUID customerId, List<Item> items, String voucherCode) {
    return place(customerId, items, voucherCode, null);
  }

  public Placed place(
      UUID customerId, List<Item> items, String voucherCode, String idempotencyKey) {
    if (idempotencyKey == null) {
      var quote = quote(customerId, items, voucherCode);
      return transactions.execute(tx -> create(quote, null));
    }
    if (idempotencyKey.isBlank() || idempotencyKey.length() > 200)
      throw new OrderRuleViolation("Idempotency-Key must be 1 to 200 characters");
    var hash = requestHash(customerId, items, voucherCode);
    // A retry is answered before anything else: not even a catalog outage can make it fail.
    var answered = keys.find(idempotencyKey);
    if (answered.isPresent()) return replay(answered.get(), hash);
    var quote = quote(customerId, items, voucherCode);
    try {
      return transactions.execute(
          tx -> create(quote, new IdempotencyKeys.Claim(idempotencyKey, hash, null, null, null)));
    } catch (KeyTaken concurrentWinner) {
      return replay(keys.find(idempotencyKey).orElseThrow(), hash);
    } catch (VoucherExhausted e) {
      // A concurrent request with the same key may have taken the voucher's last use for the
      // very order this request would have become.
      var winner = keys.find(idempotencyKey);
      if (winner.isEmpty()) throw e;
      return replay(winner.get(), hash);
    }
  }

  private Placed create(Quote quote, IdempotencyKeys.Claim key) {
    var id = UUID.randomUUID();
    var correlationId = UUID.randomUUID();
    var appended = orders.record(Order.empty(id), null, correlationId, placed(id, quote));
    var order = appended.order();
    var placedEvent = appended.events().getFirst();
    var now = clock.instant();
    sagas.start(Saga.start(id, correlationId, now, now.plus(saga.stepTimeout())));
    outbox.append(
        placedEvent.reply(
            new AuthorizePayment(
                id, order.total(), order.currency(), AuthorizePayment.keyFor(id))));
    if (key != null
        && !keys.claim(
            new IdempotencyKeys.Claim(
                key.key(), key.requestHash(), id, correlationId, placedEvent.eventId())))
      throw new KeyTaken(); // rolls back this order: the other request's order is the answer
    return answer(order, correlationId, placedEvent.eventId(), false);
  }

  /** The first request's order, as it stands now. */
  private Placed replay(IdempotencyKeys.Claim claim, String hash) {
    if (!claim.requestHash().equals(hash))
      throw new IdempotencyConflict(
          "Idempotency-Key " + claim.key() + " was already used for a different order");
    return answer(orders.load(claim.orderId()), claim.correlationId(), claim.eventId(), true);
  }

  private static Placed answer(Order order, UUID correlationId, UUID eventId, boolean replayed) {
    return new Placed(
        order.id(),
        correlationId,
        eventId,
        order.invoiceNumber(),
        order.currency(),
        order.subtotal(),
        order.discount(),
        order.tax(),
        order.total(),
        order.version(),
        replayed);
  }

  /** The request, reduced to what makes two requests "the same order". */
  static String requestHash(UUID customerId, List<Item> items, String voucherCode) {
    var canonical =
        customerId
            + "|"
            + String.join(
                ",",
                items.stream()
                    .sorted(Comparator.comparing(Item::sku))
                    .map(i -> i.sku() + ":" + i.quantity())
                    .toList())
            + "|"
            + (Voucher.normalize(voucherCode) == null ? "" : Voucher.normalize(voucherCode));
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
