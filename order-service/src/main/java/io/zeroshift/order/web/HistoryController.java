package io.zeroshift.order.web;

import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.order.application.EventStore;
import io.zeroshift.order.application.OrderRepository;
import io.zeroshift.order.application.PlaceOrder;
import io.zeroshift.order.domain.Order;
import io.zeroshift.order.infrastructure.ShippedSales;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.JsonNode;

/**
 * An order's history as the event store holds it: every event exactly as stored and as read today
 * (upcast to the current schema), the state after each, and the state rebuilt at any version or
 * point in time. Nothing here uses a snapshot.
 */
@RestController
public class HistoryController {
  /**
   * @param stored the row as written, at {@code storedVersion}
   * @param decoded the payload as today's code reads it, at {@code currentVersion}
   */
  public record StoredEvent(
      long position,
      long version,
      UUID eventId,
      String type,
      int storedVersion,
      int currentVersion,
      Instant recordedAt,
      Instant occurredAt,
      UUID correlationId,
      UUID causationId,
      JsonNode stored,
      JsonNode decoded) {}

  public record Moment(StoredEvent event, Order stateAfter) {}

  public record Rebuilt(
      Order state, Long version, Instant at, List<StoredEvent> applied, List<StoredEvent> later) {}

  private final OrderRepository orders;
  private final PlaceOrder placeOrder;
  private final ShippedSales shippedSales;

  public HistoryController(
      OrderRepository orders, PlaceOrder placeOrder, ShippedSales shippedSales) {
    this.orders = orders;
    this.placeOrder = placeOrder;
    this.shippedSales = shippedSales;
  }

  /** Units and revenue per SKU of every shipped order, straight from the event store. */
  @GetMapping("/lab/history/shipped-sales")
  public List<ShippedSales.Sales> shippedSales() {
    return shippedSales.bySku();
  }

  /** Stored events per type and schema version. */
  @GetMapping("/lab/history/versions")
  public java.util.Map<String, java.util.Map<Integer, Integer>> versions() {
    return shippedSales.versions();
  }

  @GetMapping("/orders/{id}/history")
  public List<Moment> history(@PathVariable UUID id) {
    return orders.history(id).stream()
        .map(m -> new Moment(view(m.event()), m.stateAfter()))
        .toList();
  }

  /** The order folded up to {@code version}, or up to what was recorded by {@code at}. */
  @GetMapping("/orders/{id}/rebuild")
  public Rebuilt rebuild(
      @PathVariable UUID id,
      @RequestParam(required = false) Long version,
      @RequestParam(required = false) Instant at) {
    var r = orders.rebuild(id, version, at);
    return new Rebuilt(
        r.state(),
        version,
        at,
        r.applied().stream().map(HistoryController::view).toList(),
        r.notYetApplied().stream().map(HistoryController::view).toList());
  }

  /**
   * Places an order through the normal path, but as the previous deployment wrote it: OrderPlaced
   * at schema v1 (no currency, no commerce amounts) in the event store and the outbox alike.
   * Everything that reads it afterwards, here and in other services, goes through the v1 → v2
   * upcaster and sees neutral amounts (no discount, no tax). The orders and invoice tables are
   * written from the event as placed, before encoding, so they keep what v1 cannot say: the lab can
   * compare the two.
   */
  @PostMapping("/lab/history/legacy-order")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public PlaceOrder.Placed legacyOrder(@Valid @RequestBody PlaceOrderRequest request)
      throws Exception {
    return MessageCodec.writingAs(
        1, () -> placeOrder.place(request.customerId(), request.toItems(), request.voucherCode()));
  }

  static StoredEvent view(EventStore.Recorded r) {
    var json = MessageCodec.json();
    var e = r.envelope();
    return new StoredEvent(
        r.position(),
        r.version(),
        e.eventId(),
        e.type(),
        r.storedVersion(),
        e.schemaVersion(),
        r.recordedAt(),
        e.occurredAt(),
        e.correlationId(),
        e.causationId(),
        json.readTree(r.stored()),
        json.valueToTree(e.payload()));
  }
}
