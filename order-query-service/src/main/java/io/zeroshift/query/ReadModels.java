package io.zeroshift.query;

import static io.zeroshift.query.db.Tables.CUSTOMER_SUMMARY;
import static io.zeroshift.query.db.Tables.ORDER_VIEW;
import static org.jooq.impl.DSL.excluded;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.when;

import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.contracts.OrderEvent.OrderPlaced;
import io.zeroshift.contracts.OrderLine;
import io.zeroshift.platform.PostgresClock;
import io.zeroshift.query.db.tables.records.OrderViewRecord;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.UpdateSetMoreStep;
import tools.jackson.core.type.TypeReference;

/**
 * The read side's two tables, written only by {@link OrderProjection} and read by the query API.
 * Every write joins the delivery's transaction, together with its idempotency record.
 */
public final class ReadModels {
  /** Where an event came from: stamped on the order's row as "last applied". */
  public record Position(UUID eventId, String eventType, String kafkaOffset) {}

  /** The order row a change was applied to. */
  private record Applied(String customerId, BigDecimal total) {}

  public record Stats(int orders, int customers, OffsetDateTime lastProjectedAt) {}

  private static final TypeReference<List<OrderLine>> ITEMS = new TypeReference<>() {};

  private final DSLContext db;

  public ReadModels(DSLContext db) {
    this.db = db;
  }

  /** Creates the order's row and counts it for its customer. False if it already existed. */
  public boolean placed(OrderPlaced p, Position at, Instant placedAt) {
    boolean created =
        db.insertInto(ORDER_VIEW)
                .set(ORDER_VIEW.ORDER_ID, p.orderId())
                .set(ORDER_VIEW.CUSTOMER_ID, p.customerId())
                .set(ORDER_VIEW.CUSTOMER_NAME, p.customerName())
                .set(ORDER_VIEW.STATUS, "PLACED")
                .set(ORDER_VIEW.CURRENCY, p.currency())
                .set(ORDER_VIEW.SUBTOTAL, p.subtotal())
                .set(ORDER_VIEW.DISCOUNT, p.discount())
                .set(ORDER_VIEW.TAX_RATE, p.taxRate())
                .set(ORDER_VIEW.TAX, p.tax())
                .set(ORDER_VIEW.TOTAL, p.total())
                .set(ORDER_VIEW.VOUCHER_CODE, p.voucherCode())
                .set(ORDER_VIEW.INVOICE_NUMBER, p.invoiceNumber())
                .set(ORDER_VIEW.INVOICE_STATUS, p.invoiceNumber() == null ? null : "ISSUED")
                .set(ORDER_VIEW.ITEM_COUNT, p.lines().stream().mapToInt(OrderLine::quantity).sum())
                .set(
                    ORDER_VIEW.ITEMS,
                    JSONB.valueOf(MessageCodec.json().writeValueAsString(p.lines())))
                .set(ORDER_VIEW.EVENTS_APPLIED, 1)
                .set(ORDER_VIEW.LAST_EVENT_TYPE, at.eventType())
                .set(ORDER_VIEW.LAST_EVENT_ID, at.eventId())
                .set(ORDER_VIEW.LAST_OFFSET, at.kafkaOffset())
                .set(ORDER_VIEW.PLACED_AT, placedAt.atOffset(ZoneOffset.UTC))
                .onConflictDoNothing()
                .execute()
            == 1;
    if (created)
      db.insertInto(CUSTOMER_SUMMARY)
          .set(CUSTOMER_SUMMARY.CUSTOMER_ID, p.customerId())
          .set(CUSTOMER_SUMMARY.CUSTOMER_NAME, p.customerName())
          .set(CUSTOMER_SUMMARY.CURRENCY, p.currency())
          .set(CUSTOMER_SUMMARY.ORDERS_PLACED, 1)
          .onConflict(CUSTOMER_SUMMARY.CUSTOMER_ID)
          .doUpdate()
          .set(CUSTOMER_SUMMARY.CUSTOMER_NAME, excluded(CUSTOMER_SUMMARY.CUSTOMER_NAME))
          .set(CUSTOMER_SUMMARY.ORDERS_PLACED, CUSTOMER_SUMMARY.ORDERS_PLACED.plus(1))
          .set(CUSTOMER_SUMMARY.UPDATED_AT, PostgresClock.NOW)
          .execute();
    return created;
  }

  // Each later event: its column changes, applied to the order's row. Empty if there is no row
  // (the order was never seen placed). An order placed with an invoice (every order since ADR 021)
  // has it paid with the payment and voided on cancellation; older orders never had one.

  public boolean paid(UUID orderId, UUID paymentId, Position at) {
    return advance(
            orderId,
            at,
            u ->
                u.set(ORDER_VIEW.STATUS, "PAID")
                    .set(ORDER_VIEW.PAYMENT_ID, paymentId)
                    .set(ORDER_VIEW.INVOICE_STATUS, invoiceBecomes("PAID")))
        .isPresent();
  }

  public boolean reserved(UUID orderId, UUID reservationId, Position at) {
    return advance(
            orderId,
            at,
            u -> u.set(ORDER_VIEW.STATUS, "RESERVED").set(ORDER_VIEW.RESERVATION_ID, reservationId))
        .isPresent();
  }

  public boolean shipped(UUID orderId, String trackingNumber, String carrier, Position at) {
    var applied =
        advance(
            orderId,
            at,
            u ->
                u.set(ORDER_VIEW.STATUS, "SHIPPED")
                    .set(ORDER_VIEW.TRACKING_NUMBER, trackingNumber)
                    .set(ORDER_VIEW.CARRIER, carrier));
    applied.ifPresent(
        order ->
            db.update(CUSTOMER_SUMMARY)
                .set(CUSTOMER_SUMMARY.ORDERS_SHIPPED, CUSTOMER_SUMMARY.ORDERS_SHIPPED.plus(1))
                .set(
                    CUSTOMER_SUMMARY.SHIPPED_VALUE,
                    CUSTOMER_SUMMARY.SHIPPED_VALUE.plus(order.total()))
                .set(CUSTOMER_SUMMARY.UPDATED_AT, PostgresClock.NOW)
                .where(CUSTOMER_SUMMARY.CUSTOMER_ID.eq(order.customerId()))
                .execute());
    return applied.isPresent();
  }

  public boolean cancelled(UUID orderId, String reason, List<String> compensations, Position at) {
    var applied =
        advance(
            orderId,
            at,
            u ->
                u.set(ORDER_VIEW.STATUS, "CANCELLED")
                    .set(ORDER_VIEW.CANCEL_REASON, reason)
                    .set(ORDER_VIEW.COMPENSATIONS, compensations.toArray(String[]::new))
                    .set(ORDER_VIEW.INVOICE_STATUS, invoiceBecomes("VOIDED")));
    applied.ifPresent(
        order ->
            db.update(CUSTOMER_SUMMARY)
                .set(CUSTOMER_SUMMARY.ORDERS_CANCELLED, CUSTOMER_SUMMARY.ORDERS_CANCELLED.plus(1))
                .set(CUSTOMER_SUMMARY.UPDATED_AT, PostgresClock.NOW)
                .where(CUSTOMER_SUMMARY.CUSTOMER_ID.eq(order.customerId()))
                .execute());
    return applied.isPresent();
  }

  /** The invoice's new status, if the order has an invoice. */
  private static Field<String> invoiceBecomes(String status) {
    return when(ORDER_VIEW.INVOICE_STATUS.isNotNull(), inline(status));
  }

  /** Stamps the event as the order's last applied, plus its own column changes. */
  private Optional<Applied> advance(
      UUID orderId, Position at, UnaryOperator<UpdateSetMoreStep<OrderViewRecord>> changes) {
    var update =
        db.update(ORDER_VIEW)
            .set(ORDER_VIEW.EVENTS_APPLIED, ORDER_VIEW.EVENTS_APPLIED.plus(1))
            .set(ORDER_VIEW.LAST_EVENT_TYPE, at.eventType())
            .set(ORDER_VIEW.LAST_EVENT_ID, at.eventId())
            .set(ORDER_VIEW.LAST_OFFSET, at.kafkaOffset())
            .set(ORDER_VIEW.PROJECTED_AT, PostgresClock.NOW);
    return changes
        .apply(update)
        .where(ORDER_VIEW.ORDER_ID.eq(orderId))
        .returning(ORDER_VIEW.CUSTOMER_ID, ORDER_VIEW.TOTAL)
        .fetchOptional(r -> new Applied(r.get(ORDER_VIEW.CUSTOMER_ID), r.get(ORDER_VIEW.TOTAL)));
  }

  /** Empties both read models: the start of a rebuild. */
  public void clear() {
    db.truncate(ORDER_VIEW).execute();
    db.truncate(CUSTOMER_SUMMARY).execute();
  }

  // ---- Reads -----------------------------------------------------------------------------------

  /** One order as the read side knows it; {@code eventsApplied} is its read-side version. */
  public record OrderView(
      UUID orderId,
      String customerId,
      String customerName,
      String status,
      String currency,
      BigDecimal subtotal,
      BigDecimal discount,
      BigDecimal taxRate,
      BigDecimal tax,
      BigDecimal total,
      String voucherCode,
      String invoiceNumber,
      String invoiceStatus,
      int itemCount,
      List<OrderLine> items,
      UUID paymentId,
      UUID reservationId,
      String trackingNumber,
      String carrier,
      String cancelReason,
      List<String> compensations,
      int eventsApplied,
      String lastEventType,
      UUID lastEventId,
      String lastOffset,
      OffsetDateTime placedAt,
      OffsetDateTime projectedAt) {}

  public record CustomerView(
      String customerId,
      String customerName,
      String currency,
      int ordersPlaced,
      int ordersShipped,
      int ordersCancelled,
      BigDecimal shippedValue,
      OffsetDateTime updatedAt) {}

  /** Newest first, at most {@code limit + 1} (the extra one tells a page there is more). */
  public List<OrderView> recentOrders(int limit) {
    return db.selectFrom(ORDER_VIEW)
        .orderBy(ORDER_VIEW.PLACED_AT.desc())
        .limit(limit + 1)
        .fetch(ReadModels::view);
  }

  public Optional<OrderView> order(UUID orderId) {
    return db.selectFrom(ORDER_VIEW)
        .where(ORDER_VIEW.ORDER_ID.eq(orderId))
        .fetchOptional(ReadModels::view);
  }

  /** How many of the order's events the projection has applied: its read-side version. */
  public Optional<Integer> projectedVersion(UUID orderId) {
    return db.select(ORDER_VIEW.EVENTS_APPLIED)
        .from(ORDER_VIEW)
        .where(ORDER_VIEW.ORDER_ID.eq(orderId))
        .fetchOptional(ORDER_VIEW.EVENTS_APPLIED);
  }

  public List<CustomerView> customers() {
    return db.selectFrom(CUSTOMER_SUMMARY)
        .orderBy(CUSTOMER_SUMMARY.SHIPPED_VALUE.desc(), CUSTOMER_SUMMARY.CUSTOMER_ID)
        .fetch(
            r ->
                new CustomerView(
                    r.getCustomerId(),
                    r.getCustomerName(),
                    r.getCurrency(),
                    r.getOrdersPlaced(),
                    r.getOrdersShipped(),
                    r.getOrdersCancelled(),
                    r.getShippedValue(),
                    r.getUpdatedAt()));
  }

  private static OrderView view(OrderViewRecord r) {
    return new OrderView(
        r.getOrderId(),
        r.getCustomerId(),
        r.getCustomerName(),
        r.getStatus(),
        r.getCurrency(),
        r.getSubtotal(),
        r.getDiscount(),
        r.getTaxRate(),
        r.getTax(),
        r.getTotal(),
        r.getVoucherCode(),
        r.getInvoiceNumber(),
        r.getInvoiceStatus(),
        r.getItemCount(),
        MessageCodec.json().readValue(r.getItems().data(), ITEMS),
        r.getPaymentId(),
        r.getReservationId(),
        r.getTrackingNumber(),
        r.getCarrier(),
        r.getCancelReason(),
        List.of(r.getCompensations()),
        r.getEventsApplied(),
        r.getLastEventType(),
        r.getLastEventId(),
        r.getLastOffset(),
        r.getPlacedAt(),
        r.getProjectedAt());
  }

  public Stats stats() {
    return new Stats(
        db.fetchCount(ORDER_VIEW),
        db.fetchCount(CUSTOMER_SUMMARY),
        db.select(max(ORDER_VIEW.PROJECTED_AT)).from(ORDER_VIEW).fetchSingle().value1());
  }
}
