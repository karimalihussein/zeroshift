package io.zeroshift.order.web;

import io.zeroshift.order.application.EventStore;
import io.zeroshift.order.application.OrderNotFound;
import io.zeroshift.order.application.OrderRepository;
import io.zeroshift.order.application.OrderTables;
import io.zeroshift.order.application.SagaStore;
import io.zeroshift.order.domain.Order;
import io.zeroshift.order.domain.Saga;
import io.zeroshift.order.infrastructure.PostgresLease;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;

/** The order service's response shapes, and the reads that assemble them. */
@Component
public class OrderViews {
  /**
   * {@code version}: a read-your-writes token; {@code replayed}: answered from an Idempotency-Key.
   */
  public record OrderAccepted(
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
      boolean replayed,
      String traceId) {}

  public record OrderSummary(Saga saga, Order order) {}

  public record EventView(
      long position,
      long version,
      UUID eventId,
      String type,
      int schemaVersion,
      UUID correlationId,
      UUID causationId,
      Instant recordedAt,
      Object payload) {}

  /** {@code invoice}: from the invoice table; null for an order written around the normal path. */
  public record OrderDetail(
      Order order,
      OrderTables.Invoice invoice,
      String rebuiltFrom,
      Long snapshotVersion,
      List<EventView> events,
      Saga saga,
      List<SagaStore.Transition> transitions) {}

  /** The scanner lease, plus which replica answered. */
  public record LeaseView(
      String name,
      String owner,
      Long token,
      OffsetDateTime acquiredAt,
      OffsetDateTime expiresAt,
      boolean held,
      String instance) {}

  public record RefundAccepted(UUID orderId, String command, UUID eventId) {}

  /** How far back a customer filter looks: the newest sagas only, which is what the lab needs. */
  static final int CUSTOMER_SCAN = 100;

  private final OrderRepository orders;
  private final EventStore events;
  private final SagaStore sagas;
  private final OrderTables tables;

  public OrderViews(
      OrderRepository orders, EventStore events, SagaStore sagas, OrderTables tables) {
    this.orders = orders;
    this.events = events;
    this.sagas = sagas;
    this.tables = tables;
  }

  /** Newest first, up to {@code limit + 1}; filtered to one customer among the newest sagas. */
  List<OrderSummary> recent(int limit, UUID customerId) {
    return sagas.recent(customerId == null ? limit + 1 : CUSTOMER_SCAN).stream()
        .map(s -> new OrderSummary(s, orders.load(s.orderId())))
        .filter(o -> customerId == null || customerId.toString().equals(o.order().customerId()))
        .limit(limit + 1L)
        .toList();
  }

  OrderDetail detail(UUID id, boolean useSnapshot) {
    var loaded = orders.load(id, useSnapshot);
    if (loaded.order().version() == 0) throw new OrderNotFound(id);
    var snapshot = loaded.snapshot();
    var rebuiltFrom =
        (snapshot == null ? "all " : "snapshot v" + snapshot.version() + " + ")
            + loaded.replayed().size()
            + (loaded.replayed().size() == 1 ? " event" : " events");
    return new OrderDetail(
        loaded.order(),
        tables.invoice(id).orElse(null),
        rebuiltFrom,
        snapshot == null ? null : snapshot.version(),
        events.load(id, 0).stream().map(OrderViews::view).toList(),
        sagas.find(id).orElse(null),
        sagas.transitions(id));
  }

  static LeaseView lease(PostgresLease.View lease, String instance) {
    return new LeaseView(
        lease.name(),
        lease.owner(),
        lease.token(),
        lease.acquiredAt(),
        lease.expiresAt(),
        lease.held(),
        instance);
  }

  private static EventView view(EventStore.Recorded recorded) {
    var e = recorded.envelope();
    return new EventView(
        recorded.position(),
        recorded.version(),
        e.eventId(),
        e.type(),
        e.schemaVersion(),
        e.correlationId(),
        e.causationId(),
        recorded.recordedAt(),
        e.payload());
  }
}
