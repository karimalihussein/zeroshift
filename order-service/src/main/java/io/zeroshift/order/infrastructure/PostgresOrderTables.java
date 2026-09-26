package io.zeroshift.order.infrastructure;

import static io.zeroshift.order.db.Tables.INVOICE;
import static io.zeroshift.order.db.Tables.ORDERS;
import static io.zeroshift.order.db.Tables.ORDER_ITEM;
import static io.zeroshift.order.db.Tables.VOUCHER;

import io.zeroshift.contracts.OrderEvent;
import io.zeroshift.contracts.OrderEvent.*;
import io.zeroshift.order.application.OrderTables;
import io.zeroshift.order.db.Sequences;
import io.zeroshift.order.db.tables.records.InvoiceRecord;
import io.zeroshift.order.domain.InvoiceStatus;
import io.zeroshift.order.domain.Order;
import io.zeroshift.order.domain.OrderStatus;
import io.zeroshift.platform.PostgresClock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.DatePart;
import org.jooq.TableField;
import org.jooq.impl.DSL;

/**
 * Writes each recorded event into orders, order_item and invoice, in the transaction that appended
 * it. Each row carries the stream's version, so it can always be compared with the fold it caches.
 * The invoice follows the order: issued with it, paid when the payment is authorized, voided when
 * the order is cancelled, which also gives the voucher's use back.
 */
public final class PostgresOrderTables implements OrderTables {
  private final DSLContext db;

  public PostgresOrderTables(DSLContext db) {
    this.db = db;
  }

  @Override
  public void recorded(Order after, List<OrderEvent> events) {
    for (var event : events)
      switch (event) {
        case OrderPlaced e -> placed(e, after.version());
        case OrderPaymentAuthorized e -> {
          db.update(ORDERS)
              .set(ORDERS.STATUS, OrderStatus.PAID.name())
              .set(ORDERS.PAYMENT_ID, e.paymentId())
              .set(ORDERS.PAID_AT, PostgresClock.NOW)
              .set(ORDERS.VERSION, after.version())
              .set(ORDERS.UPDATED_AT, PostgresClock.NOW)
              .where(ORDERS.ID.eq(e.orderId()))
              .execute();
          invoice(e.orderId(), InvoiceStatus.PAID, INVOICE.PAID_AT);
        }
        case OrderStockReserved e ->
            db.update(ORDERS)
                .set(ORDERS.STATUS, OrderStatus.RESERVED.name())
                .set(ORDERS.RESERVATION_ID, e.reservationId())
                .set(ORDERS.VERSION, after.version())
                .set(ORDERS.UPDATED_AT, PostgresClock.NOW)
                .where(ORDERS.ID.eq(e.orderId()))
                .execute();
        case OrderShipped e ->
            db.update(ORDERS)
                .set(ORDERS.STATUS, OrderStatus.SHIPPED.name())
                .set(ORDERS.TRACKING_NUMBER, e.trackingNumber())
                .set(ORDERS.SHIPPED_AT, PostgresClock.NOW)
                .set(ORDERS.VERSION, after.version())
                .set(ORDERS.UPDATED_AT, PostgresClock.NOW)
                .where(ORDERS.ID.eq(e.orderId()))
                .execute();
        case OrderCancelled e -> cancelled(e, after.version());
      }
  }

  private void placed(OrderPlaced e, long version) {
    var voucher =
        e.voucherCode() == null
            ? null
            : db.select(VOUCHER.ID, VOUCHER.DISCOUNT_TYPE, VOUCHER.VALUE)
                .from(VOUCHER)
                .where(VOUCHER.CODE.eq(e.voucherCode()))
                .fetchOne();
    db.insertInto(ORDERS)
        .set(ORDERS.ID, e.orderId())
        .set(ORDERS.CUSTOMER_ID, UUID.fromString(e.customerId()))
        .set(ORDERS.STATUS, OrderStatus.PLACED.name())
        .set(ORDERS.CURRENCY, e.currency())
        .set(ORDERS.SUBTOTAL, e.subtotal())
        .set(ORDERS.DISCOUNT, e.discount())
        .set(ORDERS.TAX_RATE, e.taxRate())
        .set(ORDERS.TAX, e.tax())
        .set(ORDERS.TOTAL, e.total())
        .set(ORDERS.VOUCHER_ID, voucher == null ? null : voucher.value1())
        .set(ORDERS.VOUCHER_CODE, e.voucherCode())
        .set(ORDERS.VOUCHER_DISCOUNT_TYPE, voucher == null ? null : voucher.value2())
        .set(ORDERS.VOUCHER_VALUE, voucher == null ? null : voucher.value3())
        .set(ORDERS.VERSION, version)
        .execute();
    int lineNo = 0;
    for (var line : e.lines())
      db.insertInto(ORDER_ITEM)
          .set(ORDER_ITEM.ID, UUID.randomUUID())
          .set(ORDER_ITEM.ORDER_ID, e.orderId())
          .set(ORDER_ITEM.LINE_NO, ++lineNo)
          .set(ORDER_ITEM.PRODUCT_ID, line.productId())
          .set(ORDER_ITEM.SKU, line.sku())
          .set(ORDER_ITEM.PRODUCT_NAME, line.name())
          .set(ORDER_ITEM.UNIT_PRICE, line.unitPrice())
          .set(ORDER_ITEM.QUANTITY, line.quantity())
          .set(ORDER_ITEM.SUBTOTAL, line.subtotal())
          .set(ORDER_ITEM.DISCOUNT, line.discount())
          .set(ORDER_ITEM.TOTAL, line.total())
          .execute();
    if (e.invoiceNumber() != null)
      db.insertInto(INVOICE)
          .set(INVOICE.ID, UUID.randomUUID())
          .set(INVOICE.ORDER_ID, e.orderId())
          .set(INVOICE.NUMBER, e.invoiceNumber())
          .set(INVOICE.STATUS, InvoiceStatus.ISSUED.name())
          .set(INVOICE.CURRENCY, e.currency())
          .set(INVOICE.SUBTOTAL, e.subtotal())
          .set(INVOICE.DISCOUNT, e.discount())
          .set(INVOICE.TAX, e.tax())
          .set(INVOICE.TOTAL, e.total())
          .set(INVOICE.ISSUED_AT, PostgresClock.NOW)
          .execute();
  }

  /** The status guard makes the voucher's use come back once, even if this ran twice. */
  private void cancelled(OrderCancelled e, long version) {
    int cancelled =
        db.update(ORDERS)
            .set(ORDERS.STATUS, OrderStatus.CANCELLED.name())
            .set(ORDERS.CANCEL_REASON, e.reason())
            .set(ORDERS.CANCELLED_AT, PostgresClock.NOW)
            .set(ORDERS.VERSION, version)
            .set(ORDERS.UPDATED_AT, PostgresClock.NOW)
            .where(ORDERS.ID.eq(e.orderId()))
            .and(ORDERS.STATUS.ne(OrderStatus.CANCELLED.name()))
            .execute();
    if (cancelled == 0) return;
    invoice(e.orderId(), InvoiceStatus.VOIDED, INVOICE.VOIDED_AT);
    db.update(VOUCHER)
        .set(VOUCHER.USAGE_COUNT, VOUCHER.USAGE_COUNT.minus(1))
        .set(VOUCHER.VERSION, VOUCHER.VERSION.plus(1))
        .set(VOUCHER.UPDATED_AT, PostgresClock.NOW)
        .where(
            VOUCHER.ID.eq(
                DSL.select(ORDERS.VOUCHER_ID).from(ORDERS).where(ORDERS.ID.eq(e.orderId()))))
        .and(VOUCHER.USAGE_COUNT.gt(0))
        .execute();
  }

  private void invoice(
      UUID orderId, InvoiceStatus status, TableField<InvoiceRecord, OffsetDateTime> at) {
    db.update(INVOICE)
        .set(INVOICE.STATUS, status.name())
        .set(at, PostgresClock.NOW)
        .set(INVOICE.UPDATED_AT, PostgresClock.NOW)
        .where(INVOICE.ORDER_ID.eq(orderId))
        .and(INVOICE.STATUS.ne(InvoiceStatus.VOIDED.name()))
        .execute();
  }

  /** Numbered on the database's clock, like every other timestamp in this schema. */
  @Override
  public String nextInvoiceNumber() {
    var r =
        db.select(
                Sequences.INVOICE_NUMBER_SEQ.nextval(),
                DSL.extract(PostgresClock.NOW, DatePart.YEAR))
            .fetchSingle();
    return "INV-%d-%06d".formatted(r.value2(), r.value1());
  }

  @Override
  public Optional<Invoice> invoice(UUID orderId) {
    return db.selectFrom(INVOICE)
        .where(INVOICE.ORDER_ID.eq(orderId))
        .fetchOptional(
            r ->
                new Invoice(
                    r.getId(),
                    r.getNumber(),
                    InvoiceStatus.valueOf(r.getStatus()),
                    r.getCurrency(),
                    r.getSubtotal(),
                    r.getDiscount(),
                    r.getTax(),
                    r.getTotal(),
                    instant(r.getIssuedAt()),
                    instant(r.getPaidAt()),
                    instant(r.getVoidedAt())));
  }

  private static Instant instant(OffsetDateTime time) {
    return time == null ? null : time.toInstant();
  }
}
