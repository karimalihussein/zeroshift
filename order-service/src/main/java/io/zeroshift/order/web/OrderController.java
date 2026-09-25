package io.zeroshift.order.web;

import io.zeroshift.contracts.OrderEvent;
import io.zeroshift.order.application.*;
import io.zeroshift.order.domain.Order;
import io.zeroshift.order.domain.OrderRuleViolation;
import io.zeroshift.order.domain.Saga;
import io.zeroshift.order.infrastructure.DualWriteDemo;
import io.zeroshift.order.infrastructure.PostgresLease;
import io.zeroshift.order.infrastructure.SagaTimeouts;
import io.zeroshift.platform.Traces;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestController
public class OrderController {
  public record PlaceRequest(String customerId, List<PlaceOrder.Item> items) {}

  public record Accepted(
      UUID orderId, UUID correlationId, UUID eventId, BigDecimal total, String traceId) {}

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

  public record FoldStep(long version, String type, Order stateAfter) {}

  public record OrderDetail(
      Order order,
      String rebuiltFrom,
      Long snapshotVersion,
      List<EventView> events,
      Saga saga,
      List<Map<String, Object>> transitions) {}

  public record Summary(Saga saga, Order order) {}

  private final PlaceOrder placeOrder;
  private final OrderRepository orders;
  private final EventStore events;
  private final SagaStore sagas;
  private final Catalog catalog;
  private final DualWriteDemo dualWrite;
  private final PostgresLease lease;

  public OrderController(
      PlaceOrder placeOrder,
      OrderRepository orders,
      EventStore events,
      SagaStore sagas,
      Catalog catalog,
      DualWriteDemo dualWrite,
      PostgresLease lease) {
    this.lease = lease;
    this.placeOrder = placeOrder;
    this.orders = orders;
    this.events = events;
    this.sagas = sagas;
    this.catalog = catalog;
    this.dualWrite = dualWrite;
  }

  @PostMapping("/orders")
  public ResponseEntity<Accepted> place(@RequestBody PlaceRequest request) {
    var placed = placeOrder.place(request.customerId(), request.items());
    return ResponseEntity.status(HttpStatus.ACCEPTED)
        .body(
            new Accepted(
                placed.orderId(),
                placed.correlationId(),
                placed.eventId(),
                placed.total(),
                Traces.traceId()));
  }

  /** The anti-pattern, on purpose. See {@link DualWriteDemo}. */
  @PostMapping("/orders/dual-write")
  public DualWriteDemo.Result dualWrite(
      @RequestBody PlaceRequest request, @RequestParam DualWriteDemo.Mode mode) {
    return dualWrite.place(request.customerId(), request.items(), mode);
  }

  @GetMapping("/orders")
  public List<Summary> recent(@RequestParam(defaultValue = "20") int limit) {
    return sagas.recent(Math.min(limit, 100)).stream()
        .map(s -> new Summary(s, orders.load(s.orderId())))
        .toList();
  }

  @GetMapping("/orders/{id}")
  public OrderDetail detail(
      @PathVariable UUID id, @RequestParam(defaultValue = "true") boolean useSnapshot) {
    var loaded = orders.load(id, useSnapshot);
    if (loaded.order().version() == 0)
      throw new NoSuchElementException("No events for order " + id);
    var snapshot = loaded.snapshot();
    var rebuiltFrom =
        (snapshot == null ? "all " : "snapshot v" + snapshot.version() + " + ")
            + loaded.replayed().size()
            + (loaded.replayed().size() == 1 ? " event" : " events");
    return new OrderDetail(
        loaded.order(),
        rebuiltFrom,
        snapshot == null ? null : snapshot.version(),
        events.load(id, 0).stream().map(OrderController::view).toList(),
        sagas.find(id).orElse(null),
        sagas.transitions(id));
  }

  /** Every intermediate state: the aggregate rebuilt one event at a time, no snapshot. */
  @GetMapping("/orders/{id}/fold")
  public List<FoldStep> fold(@PathVariable UUID id) {
    var steps = new ArrayList<FoldStep>();
    var order = Order.empty(id);
    for (var recorded : events.load(id, 0)) {
      order = order.apply((OrderEvent) recorded.envelope().payload());
      steps.add(new FoldStep(recorded.version(), recorded.envelope().type(), order));
    }
    return steps;
  }

  @DeleteMapping("/orders/{id}/snapshot")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public void discardSnapshot(@PathVariable UUID id) {
    events.discardSnapshot(id);
  }

  /** Who currently runs the saga-timeout scanner, and with which fencing token. */
  @GetMapping("/lab/lease")
  public Map<String, Object> lease() {
    var lease = this.lease.describe(SagaTimeouts.LEASE);
    var result = new LinkedHashMap<String, Object>(lease);
    result.put("instance", this.lease.owner());
    return result;
  }

  @GetMapping("/catalog")
  public List<Catalog.Product> catalog() {
    return catalog.all();
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

  @ExceptionHandler(OrderRuleViolation.class)
  ProblemDetail rejected(OrderRuleViolation e) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_CONTENT, e.getMessage());
  }

  @ExceptionHandler(ConcurrencyConflict.class)
  ProblemDetail conflict(ConcurrencyConflict e) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
  }

  @ExceptionHandler(NoSuchElementException.class)
  ProblemDetail missing(NoSuchElementException e) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
  }
}
