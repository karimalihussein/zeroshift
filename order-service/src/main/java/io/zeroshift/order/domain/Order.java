package io.zeroshift.order.domain;

import io.zeroshift.contracts.OrderEvent;
import io.zeroshift.contracts.OrderEvent.*;
import io.zeroshift.contracts.OrderLine;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/**
 * The event-sourced order aggregate. Its state is never stored, only derived: {@link #apply} folds
 * one event into a new immutable value, and the current order is the fold of its whole history (or
 * of a snapshot plus the events after it). Command methods check the rules against the current
 * state and return the event to record; they never mutate anything.
 */
public record Order(
    UUID id,
    String customerId,
    String customerName,
    List<OrderLine> lines,
    String currency,
    BigDecimal subtotal,
    BigDecimal discount,
    BigDecimal taxRate,
    BigDecimal tax,
    BigDecimal total,
    String voucherCode,
    String invoiceNumber,
    OrderStatus status,
    UUID paymentId,
    UUID reservationId,
    String trackingNumber,
    String cancelReason,
    long version) {

  public static Order empty(UUID id) {
    return new Order(
        id,
        null,
        null,
        List.of(),
        null,
        BigDecimal.ZERO,
        BigDecimal.ZERO,
        BigDecimal.ZERO,
        BigDecimal.ZERO,
        BigDecimal.ZERO,
        null,
        null,
        OrderStatus.NEW,
        null,
        null,
        null,
        null,
        0);
  }

  /**
   * The order as sold: {@code priced} holds every amount and line snapshot, so nothing about this
   * order depends on the catalog or the voucher after this event.
   */
  public static OrderPlaced place(
      UUID id,
      String customerId,
      String customerName,
      Pricing.Priced priced,
      String voucherCode,
      String invoiceNumber) {
    if (customerId == null || customerId.isBlank())
      throw new OrderRuleViolation("An order needs a customer");
    if (priced.lines().isEmpty()) throw new OrderRuleViolation("An order needs a line");
    return new OrderPlaced(
        id,
        customerId,
        customerName,
        priced.lines(),
        priced.currency(),
        priced.subtotal(),
        priced.discount(),
        priced.taxRate(),
        priced.tax(),
        priced.total(),
        voucherCode,
        invoiceNumber);
  }

  public OrderPaymentAuthorized authorizePayment(UUID paymentId) {
    require(status == OrderStatus.PLACED, "record a payment");
    return new OrderPaymentAuthorized(id, paymentId);
  }

  public OrderStockReserved reserveStock(UUID reservationId) {
    require(status == OrderStatus.PAID, "reserve stock");
    return new OrderStockReserved(id, reservationId);
  }

  public OrderShipped ship(String trackingNumber, String carrier) {
    require(status == OrderStatus.RESERVED, "ship");
    return new OrderShipped(id, trackingNumber, carrier);
  }

  public OrderCancelled cancel(String reason, List<String> compensations) {
    require(
        status != OrderStatus.SHIPPED
            && status != OrderStatus.CANCELLED
            && status != OrderStatus.NEW,
        "cancel");
    return new OrderCancelled(id, reason, List.copyOf(compensations));
  }

  public Order apply(OrderEvent event) {
    if (!event.orderId().equals(id))
      throw new IllegalArgumentException(
          "Event for order " + event.orderId() + " applied to " + id);
    long next = version + 1;
    return switch (event) {
      case OrderPlaced e ->
          new Order(
              id,
              e.customerId(),
              e.customerName(),
              e.lines(),
              e.currency(),
              e.subtotal(),
              e.discount(),
              e.taxRate(),
              e.tax(),
              e.total(),
              e.voucherCode(),
              e.invoiceNumber(),
              OrderStatus.PLACED,
              null,
              null,
              null,
              null,
              next);
      case OrderPaymentAuthorized e ->
          with(OrderStatus.PAID, e.paymentId(), reservationId, trackingNumber, cancelReason, next);
      case OrderStockReserved e ->
          with(
              OrderStatus.RESERVED,
              paymentId,
              e.reservationId(),
              trackingNumber,
              cancelReason,
              next);
      case OrderShipped e ->
          with(
              OrderStatus.SHIPPED,
              paymentId,
              reservationId,
              e.trackingNumber(),
              cancelReason,
              next);
      case OrderCancelled e ->
          with(OrderStatus.CANCELLED, paymentId, reservationId, trackingNumber, e.reason(), next);
    };
  }

  /** After placement only the lifecycle changes; what was sold never does. */
  private Order with(
      OrderStatus status,
      UUID paymentId,
      UUID reservationId,
      String trackingNumber,
      String cancelReason,
      long version) {
    return new Order(
        id,
        customerId,
        customerName,
        lines,
        currency,
        subtotal,
        discount,
        taxRate,
        tax,
        total,
        voucherCode,
        invoiceNumber,
        status,
        paymentId,
        reservationId,
        trackingNumber,
        cancelReason,
        version);
  }

  private void require(boolean allowed, String action) {
    if (!allowed) throw new OrderRuleViolation("Cannot " + action + " an order that is " + status);
  }
}
