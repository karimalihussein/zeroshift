package io.zeroshift.contracts;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/** Facts about an order. They are its event-sourced history and the read model's only input. */
public sealed interface OrderEvent extends Message {
  /**
   * The order as sold: every price, name and amount is a snapshot. {@code total = subtotal -
   * discount + tax}; {@code taxRate} is the rate that produced {@code tax}.
   *
   * <p>Schema v2 added {@code currency}. The commerce model (ADR 021) then added {@code
   * customerName}, {@code subtotal}, {@code discount}, {@code taxRate}, {@code tax}, {@code
   * voucherCode}, {@code invoiceNumber} and the lines' product id, name and amounts: additions, so
   * still v2. An event written before them reads with neutral values (subtotal = total, no
   * discount, no tax, the customer id as name), which is exactly what those orders were.
   */
  record OrderPlaced(
      UUID orderId,
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
      String invoiceNumber)
      implements OrderEvent {
    public OrderPlaced {
      lines = List.copyOf(lines);
      if (!Money.isCurrency(currency))
        throw new IllegalArgumentException("Bad currency " + currency);
      if (customerName == null || customerName.isBlank()) customerName = customerId;
      total = Money.exact(total);
      subtotal = subtotal == null ? total : Money.exact(subtotal);
      discount = discount == null ? Money.ZERO : Money.exact(discount);
      tax = tax == null ? Money.ZERO : Money.exact(tax);
      if (taxRate == null) taxRate = BigDecimal.ZERO;
      if (total.compareTo(subtotal.subtract(discount).add(tax)) != 0)
        throw new IllegalArgumentException("total must be subtotal - discount + tax");
    }
  }

  record OrderPaymentAuthorized(UUID orderId, UUID paymentId) implements OrderEvent {}

  record OrderStockReserved(UUID orderId, UUID reservationId) implements OrderEvent {}

  record OrderShipped(UUID orderId, String trackingNumber, String carrier) implements OrderEvent {}

  /** {@code compensations} names the completed steps that were undone, in the order undone. */
  record OrderCancelled(UUID orderId, String reason, List<String> compensations)
      implements OrderEvent {}
}
