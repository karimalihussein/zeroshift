package io.zeroshift.payment.application;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.Message;
import io.zeroshift.contracts.Money;
import io.zeroshift.contracts.PaymentCommand.*;
import io.zeroshift.contracts.PaymentEvent.*;
import io.zeroshift.payment.application.Payments.Payment;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Handled;
import io.zeroshift.platform.Outbox;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Objects;

/**
 * Answers payment commands. An authorization first claims its idempotency key; a command whose key
 * is already decided is answered from that payment, so the same key never charges twice, whatever
 * the command's event id. Different keys are different payments, even for the same order.
 */
public final class PaymentHandler {
  /** Operator fault for this service: {@code decline} refuses the next authorizations. */
  public static final String DECLINE_FAULT = "payment-decline";

  private final Payments payments;
  private final PaymentGateway gateway;
  private final Outbox outbox;
  private final Faults faults;
  private final BigDecimal limit;

  public PaymentHandler(
      Payments payments, PaymentGateway gateway, Outbox outbox, Faults faults, BigDecimal limit) {
    this.payments = payments;
    this.gateway = gateway;
    this.outbox = outbox;
    this.faults = faults;
    this.limit = limit;
  }

  public Handled handle(Envelope command) {
    return switch (command.payload()) {
      case AuthorizePayment a -> authorize(command, a);
      case RefundPayment r -> refund(command, r);
      default -> Handled.ignored("Not a payment command: " + command.type());
    };
  }

  private Handled authorize(Envelope command, AuthorizePayment a) {
    var p = payments.claim(a.idempotencyKey(), a.orderId(), Money.of(a.amount()), a.currency());
    return switch (p.status()) {
      case AUTHORIZED ->
          reply(
              command,
              authorized(p),
              "Key " + p.idempotencyKey() + " already authorized: answered, no second charge");
      case DECLINED ->
          reply(
              command,
              new PaymentDeclined(p.orderId(), p.failureReason()),
              "Key " + p.idempotencyKey() + " already declined");
      case REFUNDED, VOIDED ->
          Handled.ignored("Payment " + p.idempotencyKey() + " was refunded or voided before this");
      // New, or claimed by a delivery that crashed before recording: (re)send with the same key.
      case PENDING -> charge(command, p);
    };
  }

  private Handled charge(Envelope command, Payment p) {
    if (faults.trigger(DECLINE_FAULT).isPresent())
      return decline(command, p, "Declined by operator fault");
    if (p.amount().compareTo(limit) > 0)
      return decline(command, p, "Amount " + p.amount() + " exceeds the " + limit + " limit");
    return switch (gateway.charge(p.orderId(), p.idempotencyKey(), p.amount(), p.currency())) {
      case PaymentGateway.Charged c -> {
        payments.authorize(p.id(), c.reference());
        yield reply(
            command,
            authorized(p),
            "Charged " + p.amount() + " " + p.currency() + " (" + c.reference() + ")");
      }
      case PaymentGateway.Declined d -> decline(command, p, "Gateway declined: " + d.reason());
    };
  }

  private Handled decline(Envelope command, Payment p, String reason) {
    payments.decline(p.id(), reason);
    return reply(command, new PaymentDeclined(p.orderId(), reason), reason);
  }

  /**
   * Refunds every authorized payment of the order, one reply each. With none, the order's saga key
   * is voided, so a late authorization is refused instead of charging a cancelled order, and the
   * reply refunds zero (in the order's currency when an earlier attempt carried one, else null).
   */
  private Handled refund(Envelope command, RefundPayment r) {
    payments.voidUnclaimed(AuthorizePayment.keyFor(r.orderId()), r.orderId(), r.reason());
    var all = payments.forOrder(r.orderId());
    var refunded = new ArrayList<Payment>();
    for (var p : all) {
      switch (p.status()) {
        case AUTHORIZED -> {
          payments.refund(p.id());
          refunded.add(p);
        }
        case REFUNDED -> refunded.add(p);
        case PENDING -> payments.voidPending(p.id());
        case DECLINED, VOIDED -> {}
      }
    }
    if (refunded.isEmpty())
      return reply(
          command,
          new PaymentRefunded(
              r.orderId(),
              null,
              Money.ZERO,
              all.stream()
                  .map(Payment::currency)
                  .filter(Objects::nonNull)
                  .findFirst()
                  .orElse(null)),
          "Nothing charged: order voided so a late authorization is refused");
    for (var p : refunded)
      outbox.append(
          command.reply(new PaymentRefunded(p.orderId(), p.id(), p.amount(), p.currency())));
    return Handled.processed(
        "Refunded "
            + refunded.stream().map(p -> p.amount() + " " + p.currency()).toList()
            + " ("
            + r.reason()
            + ")");
  }

  private static PaymentAuthorized authorized(Payment p) {
    return new PaymentAuthorized(p.orderId(), p.id(), p.amount(), p.currency());
  }

  private Handled reply(Envelope command, Message reply, String detail) {
    outbox.append(command.reply(reply));
    return Handled.processed(detail);
  }
}
