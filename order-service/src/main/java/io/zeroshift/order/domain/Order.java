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
    List<OrderLine> lines,
    BigDecimal total,
    String currency,
    OrderStatus status,
    UUID paymentId,
    UUID reservationId,
    String trackingNumber,
    String cancelReason,
    long version) {

  public static Order empty(UUID id) {
    return new Order(
        id, null, List.of(), BigDecimal.ZERO, null, OrderStatus.NEW, null, null, null, null, 0);
  }

  public static OrderPlaced place(UUID id, String customerId, List<OrderLine> lines) {
    if (customerId == null || customerId.isBlank())
      throw new OrderRuleViolation("An order needs a customer");
    if (lines == null || lines.isEmpty()) throw new OrderRuleViolation("An order needs a line");
    var total = lines.stream().map(OrderLine::subtotal).reduce(BigDecimal.ZERO, BigDecimal::add);
    return new OrderPlaced(id, customerId, List.copyOf(lines), total, "USD");
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
              e.lines(),
              e.total(),
              e.currency(),
              OrderStatus.PLACED,
              null,
              null,
              null,
              null,
              next);
      case OrderPaymentAuthorized e ->
          new Order(
              id,
              customerId,
              lines,
              total,
              currency,
              OrderStatus.PAID,
              e.paymentId(),
              reservationId,
              trackingNumber,
              cancelReason,
              next);
      case OrderStockReserved e ->
          new Order(
              id,
              customerId,
              lines,
              total,
              currency,
              OrderStatus.RESERVED,
              paymentId,
              e.reservationId(),
              trackingNumber,
              cancelReason,
              next);
      case OrderShipped e ->
          new Order(
              id,
              customerId,
              lines,
              total,
              currency,
              OrderStatus.SHIPPED,
              paymentId,
              reservationId,
              e.trackingNumber(),
              cancelReason,
              next);
      case OrderCancelled e ->
          new Order(
              id,
              customerId,
              lines,
              total,
              currency,
              OrderStatus.CANCELLED,
              paymentId,
              reservationId,
              trackingNumber,
              e.reason(),
              next);
    };
  }

  private void require(boolean allowed, String action) {
    if (!allowed) throw new OrderRuleViolation("Cannot " + action + " an order that is " + status);
  }
}
