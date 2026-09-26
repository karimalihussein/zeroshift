package io.zeroshift.order.application;

import io.zeroshift.contracts.*;
import io.zeroshift.contracts.InventoryCommand.*;
import io.zeroshift.contracts.InventoryEvent.*;
import io.zeroshift.contracts.PaymentCommand.*;
import io.zeroshift.contracts.PaymentEvent.*;
import io.zeroshift.contracts.ShippingCommand.*;
import io.zeroshift.contracts.ShippingEvent.*;
import io.zeroshift.order.domain.*;
import io.zeroshift.platform.Handled;
import io.zeroshift.platform.Outbox;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * The order saga orchestrator. Each reply advances the saga one step: it records the order event,
 * sends the next command through the outbox and saves the saga, all in the reply's transaction.
 *
 * <pre>
 * AWAITING_PAYMENT ─authorized─► AWAITING_STOCK ─reserved─► AWAITING_SHIPMENT ─scheduled─► COMPLETED
 *        │ declined                  │ rejected / timeout        │ failed
 *        ▼                           ▼                           ▼
 *    CANCELLED ◄──all compensations── COMPENSATING ◄───── refund payment (+ release stock)
 * </pre>
 */
public final class OrderSaga {
  private final OrderRepository orders;
  private final SagaStore sagas;
  private final Outbox outbox;
  private final Duration stepTimeout;
  private final Clock clock;

  public OrderSaga(
      OrderRepository orders, SagaStore sagas, Outbox outbox, Duration stepTimeout, Clock clock) {
    this.orders = orders;
    this.sagas = sagas;
    this.outbox = outbox;
    this.stepTimeout = stepTimeout;
    this.clock = clock;
  }

  public Duration stepTimeout() {
    return stepTimeout;
  }

  public Handled handle(Envelope reply) {
    var found = sagas.find(reply.orderId());
    if (found.isEmpty()) return Handled.ignored("No saga for order " + reply.orderId());
    var saga = found.get();
    var state = saga.state();
    return switch (reply.payload()) {
      case PaymentAuthorized p when state == SagaState.AWAITING_PAYMENT -> {
        var order = orders.load(saga.orderId());
        order = orders.record(order, reply, null, order.authorizePayment(p.paymentId())).order();
        send(
            reply,
            new ReserveStock(
                order.id(),
                order.lines().stream()
                    .map(l -> new StockLine(l.productId(), l.sku(), l.quantity()))
                    .toList()));
        save(saga, saga.advance(SagaState.AWAITING_STOCK, deadline()), reply, "payment authorized");
        yield Handled.processed("Payment " + p.paymentId() + " authorized → ReserveStock sent");
      }
      case PaymentDeclined d when state == SagaState.AWAITING_PAYMENT -> {
        var order = orders.load(saga.orderId());
        orders.record(
            order, reply, null, order.cancel("Payment declined: " + d.reason(), List.of()));
        save(saga, saga.fail("Payment declined: " + d.reason()), reply, "nothing to compensate");
        yield Handled.processed("Payment declined → order cancelled, nothing to undo");
      }
      case StockReserved s when state == SagaState.AWAITING_STOCK -> {
        var order = orders.load(saga.orderId());
        order = orders.record(order, reply, null, order.reserveStock(s.reservationId())).order();
        send(reply, new ScheduleShipment(order.id(), order.customerId()));
        save(saga, saga.advance(SagaState.AWAITING_SHIPMENT, null), reply, "stock reserved");
        yield Handled.processed("Stock reserved → ScheduleShipment sent");
      }
      case StockRejected r when state == SagaState.AWAITING_STOCK -> {
        send(reply, new RefundPayment(saga.orderId(), "Stock rejected: " + r.reason()));
        save(
            saga,
            saga.compensate("Stock rejected: " + r.reason(), Set.of("PaymentRefunded")),
            reply,
            "compensating payment");
        yield Handled.processed("Stock rejected → RefundPayment sent (compensation)");
      }
      case ShipmentScheduled s when state == SagaState.AWAITING_SHIPMENT -> {
        var order = orders.load(saga.orderId());
        orders.record(order, reply, null, order.ship(s.trackingNumber(), s.carrier()));
        save(saga, saga.advance(SagaState.COMPLETED, null), reply, "shipped " + s.trackingNumber());
        yield Handled.processed("Shipment " + s.trackingNumber() + " scheduled → saga completed");
      }
      case ShipmentFailed f when state == SagaState.AWAITING_SHIPMENT -> {
        send(reply, new ReleaseStock(saga.orderId()));
        send(reply, new RefundPayment(saga.orderId(), "Shipment failed: " + f.reason()));
        save(
            saga,
            saga.compensate(
                "Shipment failed: " + f.reason(), Set.of("StockReleased", "PaymentRefunded")),
            reply,
            "compensating stock and payment");
        yield Handled.processed("Shipment failed → ReleaseStock + RefundPayment sent");
      }
      case PaymentRefunded r when awaiting(saga, reply) ->
          compensated(saga, reply, "payment refunded");
      case StockReleased r when awaiting(saga, reply) -> compensated(saga, reply, "stock released");
      default -> Handled.ignored(state + " saga does not expect " + reply.type());
    };
  }

  /**
   * A compensatable step got no answer in time. Undo everything that may have happened, including a
   * reply still in flight: the compensation commands share the order's partition, so they are
   * processed after the original command.
   */
  public void timeout(Saga saga) {
    var reason = "Timed out after " + stepTimeout.toSeconds() + " s " + saga.state();
    var cause = Envelope.of(new RefundPayment(saga.orderId(), reason), saga.correlationId(), null);
    if (saga.state() == SagaState.AWAITING_STOCK) {
      outbox.append(Envelope.of(new ReleaseStock(saga.orderId()), saga.correlationId(), null));
      outbox.append(cause);
      save(
          saga,
          saga.compensate(reason, Set.of("StockReleased", "PaymentRefunded")),
          null,
          "timeout: compensating stock and payment");
    } else {
      outbox.append(cause);
      save(
          saga,
          saga.compensate(reason, Set.of("PaymentRefunded")),
          null,
          "timeout: compensating payment");
    }
  }

  private boolean awaiting(Saga saga, Envelope reply) {
    return saga.state() == SagaState.COMPENSATING && saga.pending().contains(reply.type());
  }

  private Handled compensated(Saga saga, Envelope reply, String undone) {
    var next = saga.compensated(reply.type(), undone);
    if (!next.pending().isEmpty()) {
      save(saga, next, reply, undone + "; still awaiting " + next.pending());
      return Handled.processed(undone + "; awaiting " + next.pending());
    }
    var order = orders.load(saga.orderId());
    orders.record(order, reply, null, order.cancel(saga.failureReason(), next.compensations()));
    save(saga, next.cancelled(), reply, "all compensations done");
    return Handled.processed(undone + " → all compensations done, order cancelled");
  }

  private void send(Envelope cause, Message command) {
    outbox.append(cause.reply(command));
  }

  private void save(Saga from, Saga to, Envelope trigger, String detail) {
    sagas.save(
        to,
        from.state().name(),
        trigger == null ? "Timeout" : trigger.type(),
        trigger == null ? null : trigger.eventId(),
        detail);
  }

  private java.time.Instant deadline() {
    return clock.instant().plus(stepTimeout);
  }
}
