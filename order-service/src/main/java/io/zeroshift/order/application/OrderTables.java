package io.zeroshift.order.application;

import io.zeroshift.contracts.OrderEvent;
import io.zeroshift.order.domain.InvoiceStatus;
import io.zeroshift.order.domain.Order;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * The relational copy of each order: orders, order_item and invoice, kept in step with the event
 * store by writing them in the same transaction as every appended event. The events stay the truth;
 * these tables give invoices, vouchers and customers real foreign keys.
 */
public interface OrderTables {
  record Invoice(
      UUID id,
      String number,
      InvoiceStatus status,
      String currency,
      BigDecimal subtotal,
      BigDecimal discount,
      BigDecimal tax,
      BigDecimal total,
      Instant issuedAt,
      Instant paidAt,
      Instant voidedAt) {}

  /** {@code events} were just appended; {@code after} is the order they produced. */
  void recorded(Order after, List<OrderEvent> events);

  /** INV-{year}-{6 digits}, unique and increasing (with gaps where a placement rolled back). */
  String nextInvoiceNumber();

  Optional<Invoice> invoice(UUID orderId);
}
