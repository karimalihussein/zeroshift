package io.zeroshift.contracts;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/** Facts about an order. They are its event-sourced history and the read model's only input. */
public sealed interface OrderEvent extends Message {
  /** Schema v2 added {@code currency}; v1 events are upcast with the lab's only currency, USD. */
  record OrderPlaced(
      UUID orderId, String customerId, List<OrderLine> lines, BigDecimal total, String currency)
      implements OrderEvent {}

  record OrderPaymentAuthorized(UUID orderId, UUID paymentId) implements OrderEvent {}

  record OrderStockReserved(UUID orderId, UUID reservationId) implements OrderEvent {}

  record OrderShipped(UUID orderId, String trackingNumber, String carrier) implements OrderEvent {}

  /** {@code compensations} names the completed steps that were undone, in the order undone. */
  record OrderCancelled(UUID orderId, String reason, List<String> compensations)
      implements OrderEvent {}
}
