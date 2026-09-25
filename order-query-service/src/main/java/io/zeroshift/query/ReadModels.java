package io.zeroshift.query;

import static io.zeroshift.query.db.Tables.CUSTOMER_SUMMARY;
import static io.zeroshift.query.db.Tables.ORDER_VIEW;
import static org.jooq.impl.DSL.max;

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
import org.jooq.JSONB;
import org.jooq.JSONFormat;
import org.jooq.UpdateSetMoreStep;

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

  // Column names become the JSON keys (snake_case); jsonb and arrays stay real JSON values.
  private static final JSONFormat JSON =
      new JSONFormat().header(false).recordFormat(JSONFormat.RecordFormat.OBJECT);

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
                .set(ORDER_VIEW.STATUS, "PLACED")
                .set(ORDER_VIEW.TOTAL, p.total())
                .set(ORDER_VIEW.CURRENCY, p.currency())
                .set(ORDER_VIEW.ITEM_COUNT, p.lines().stream().mapToInt(OrderLine::quantity).sum())
                .set(
                    ORDER_VIEW.LINES,
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
          .set(CUSTOMER_SUMMARY.ORDERS_PLACED, 1)
          .onConflict(CUSTOMER_SUMMARY.CUSTOMER_ID)
          .doUpdate()
          .set(CUSTOMER_SUMMARY.ORDERS_PLACED, CUSTOMER_SUMMARY.ORDERS_PLACED.plus(1))
          .set(CUSTOMER_SUMMARY.UPDATED_AT, PostgresClock.NOW)
          .execute();
    return created;
  }

  // Each later event: its column changes, applied to the order's row. Empty if there is no row
  // (the order was never seen placed).

  public boolean paid(UUID orderId, UUID paymentId, Position at) {
    return advance(
            orderId,
            at,
            u -> u.set(ORDER_VIEW.STATUS, "PAID").set(ORDER_VIEW.PAYMENT_ID, paymentId))
        .isPresent();
  }

  public boolean reserved(UUID orderId, UUID reservationId, Position at) {
    return advance(
            orderId,
            at,
            u -> u.set(ORDER_VIEW.STATUS, "RESERVED").set(ORDER_VIEW.RESERVATION_ID, reservationId))
        .isPresent();
  }

  public boolean shipped(UUID orderId, String trackingNumber, Position at) {
    var applied =
        advance(
            orderId,
            at,
            u ->
                u.set(ORDER_VIEW.STATUS, "SHIPPED")
                    .set(ORDER_VIEW.TRACKING_NUMBER, trackingNumber));
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
                    .set(ORDER_VIEW.COMPENSATIONS, compensations.toArray(String[]::new)));
    applied.ifPresent(
        order ->
            db.update(CUSTOMER_SUMMARY)
                .set(CUSTOMER_SUMMARY.ORDERS_CANCELLED, CUSTOMER_SUMMARY.ORDERS_CANCELLED.plus(1))
                .set(CUSTOMER_SUMMARY.UPDATED_AT, PostgresClock.NOW)
                .where(CUSTOMER_SUMMARY.CUSTOMER_ID.eq(order.customerId()))
                .execute());
    return applied.isPresent();
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

  // ---- Reads, as JSON -------------------------------------------------------------------------

  public String recentOrdersJson(int limit) {
    return db.selectFrom(ORDER_VIEW)
        .orderBy(ORDER_VIEW.PLACED_AT.desc())
        .limit(limit)
        .fetch()
        .formatJSON(JSON);
  }

  /** How many of the order's events the projection has applied: its read-side version. */
  public Optional<Integer> projectedVersion(UUID orderId) {
    return db.select(ORDER_VIEW.EVENTS_APPLIED)
        .from(ORDER_VIEW)
        .where(ORDER_VIEW.ORDER_ID.eq(orderId))
        .fetchOptional(ORDER_VIEW.EVENTS_APPLIED);
  }

  public Optional<String> orderJson(UUID orderId) {
    return db.selectFrom(ORDER_VIEW)
        .where(ORDER_VIEW.ORDER_ID.eq(orderId))
        .fetchOptional()
        .map(r -> r.formatJSON(JSON));
  }

  public String customersJson() {
    return db.selectFrom(CUSTOMER_SUMMARY)
        .orderBy(CUSTOMER_SUMMARY.SHIPPED_VALUE.desc(), CUSTOMER_SUMMARY.CUSTOMER_ID)
        .fetch()
        .formatJSON(JSON);
  }

  public Stats stats() {
    return new Stats(
        db.fetchCount(ORDER_VIEW),
        db.fetchCount(CUSTOMER_SUMMARY),
        db.select(max(ORDER_VIEW.PROJECTED_AT)).from(ORDER_VIEW).fetchSingle().value1());
  }
}
