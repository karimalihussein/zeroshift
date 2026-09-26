package io.zeroshift.order.web;

import io.zeroshift.order.application.*;
import io.zeroshift.order.infrastructure.DualWriteDemo;
import io.zeroshift.order.infrastructure.PostgresLease;
import io.zeroshift.order.infrastructure.SagaTimeouts;
import io.zeroshift.platform.Traces;
import io.zeroshift.platform.web.ApiHeaders;
import io.zeroshift.platform.web.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class OrderController {
  private final PlaceOrder placeOrder;
  private final OrderRepository orders;
  private final EventStore events;
  private final DualWriteDemo dualWrite;
  private final PostgresLease lease;
  private final OperatorRefund refunds;
  private final OrderViews views;
  private final SlowAnswers slowAnswers;
  private final EdgeGuards guards;

  public OrderController(
      PlaceOrder placeOrder,
      OrderRepository orders,
      EventStore events,
      DualWriteDemo dualWrite,
      PostgresLease lease,
      OperatorRefund refunds,
      OrderViews views,
      SlowAnswers slowAnswers,
      EdgeGuards guards) {
    this.placeOrder = placeOrder;
    this.orders = orders;
    this.events = events;
    this.dualWrite = dualWrite;
    this.lease = lease;
    this.refunds = refunds;
    this.views = views;
    this.slowAnswers = slowAnswers;
    this.guards = guards;
  }

  /**
   * Places an order. With an Idempotency-Key, a retried request gets the first request's answer
   * (Idempotent-Replayed: true) instead of creating a second order. The edge guards may refuse it
   * first (429 or 503 with Retry-After) when switched on.
   */
  @PostMapping("/orders")
  public ResponseEntity<OrderViews.OrderAccepted> place(
      @Valid @RequestBody PlaceOrderRequest request,
      @RequestHeader(name = ApiHeaders.IDEMPOTENCY_KEY, required = false) @Size(min = 1, max = 200)
          String idempotencyKey)
      throws Exception {
    var placed =
        guards.admit(
            () ->
                placeOrder.place(
                    request.customerId(),
                    request.toItems(),
                    request.voucherCode(),
                    idempotencyKey));
    slowAnswers.holdIfArmed();
    return ResponseEntity.status(HttpStatus.ACCEPTED)
        .header(ApiHeaders.IDEMPOTENT_REPLAYED, String.valueOf(placed.replayed()))
        .body(
            new OrderViews.OrderAccepted(
                placed.orderId(),
                placed.correlationId(),
                placed.eventId(),
                placed.invoiceNumber(),
                placed.currency(),
                placed.subtotal(),
                placed.discount(),
                placed.tax(),
                placed.total(),
                placed.version(),
                placed.replayed(),
                Traces.traceId()));
  }

  /** The anti-pattern, on purpose. See {@link DualWriteDemo}. */
  @PostMapping("/orders/dual-write")
  public DualWriteDemo.Result dualWrite(
      @Valid @RequestBody PlaceOrderRequest request, @RequestParam DualWriteDemo.Mode mode) {
    return dualWrite.place(request.customerId(), request.toItems(), request.voucherCode(), mode);
  }

  @GetMapping("/orders")
  public ApiResponse<List<OrderViews.OrderSummary>> recent(
      @RequestParam(defaultValue = "20") @Min(1) @Max(100) int limit,
      @RequestParam(required = false) UUID customerId) {
    return ApiResponse.page(views.recent(limit, customerId), limit);
  }

  @GetMapping("/orders/{id}")
  public OrderViews.OrderDetail detail(
      @PathVariable UUID id, @RequestParam(defaultValue = "true") boolean useSnapshot) {
    return views.detail(id, useSnapshot);
  }

  /** Every intermediate state: the aggregate rebuilt one event at a time, no snapshot. */
  @GetMapping("/orders/{id}/fold")
  public List<OrderRepository.Step> fold(@PathVariable UUID id) {
    return orders.fold(id);
  }

  @DeleteMapping("/orders/{id}/snapshot")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public void discardSnapshot(@PathVariable UUID id) {
    events.discardSnapshot(id);
  }

  /** Recovery: refund an order's payment through a real RefundPayment command. */
  @PostMapping("/orders/{id}/refund")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public OrderViews.RefundAccepted refund(
      @PathVariable UUID id,
      @RequestParam(defaultValue = "operator refund") @NotBlank @Size(max = 200) String reason) {
    var command = refunds.refund(id, reason);
    return new OrderViews.RefundAccepted(id, command.type(), command.eventId());
  }

  /** Who currently runs the saga-timeout scanner, and with which fencing token. */
  @GetMapping("/lab/lease")
  public OrderViews.LeaseView lease() {
    return OrderViews.lease(lease.describe(SagaTimeouts.LEASE), lease.owner());
  }
}
